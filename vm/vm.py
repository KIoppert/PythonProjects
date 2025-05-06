"""
Simplified VM code which works for some cases.
You need extend/rewrite code to pass all cases.
"""

import builtins
import dis
import operator
import types
import typing as tp

ERR_TOO_MANY_POS_ARGS = "Too many positional arguments"
ERR_TOO_MANY_KW_ARGS = "Too many keyword arguments"
ERR_MULT_VALUES_FOR_ARG = "Multiple values for arguments"
ERR_MISSING_POS_ARGS = "Missing positional arguments"
ERR_MISSING_KWONLY_ARGS = "Missing keyword-only arguments"
ERR_POSONLY_PASSED_AS_KW = "Positional-only argument passed as keyword argument"

BINARY_OPS = {
    "+": operator.add,
    "+=": operator.add,
    "-": operator.sub,
    "-=": operator.sub,
    "*": operator.mul,
    "*=": operator.mul,
    "/": operator.truediv,
    "/=": operator.truediv,
    "**": operator.pow,
    "**=": operator.pow,
    "//": operator.floordiv,
    "//=": operator.floordiv,
    "%": operator.mod,
    "%=": operator.mod,
    "<<": operator.lshift,
    "<<=": operator.lshift,
    ">>": operator.rshift,
    ">>=": operator.rshift,
    "&": operator.and_,
    "&=": operator.and_,
    "|": operator.or_,
    "|=": operator.or_,
    "^": operator.xor,
    "^=": operator.xor,
}
COMPARE_OPS = {
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}


class Frame:
    """
    Frame header in cpython with description
        https://github.com/python/cpython/blob/3.12/Include/internal/pycore_frame.h

    Text description of frame parameters
        https://docs.python.org/3/library/inspect.html?highlight=frame#types-and-members
    """

    def __init__(
            self,
            frame_code: types.CodeType,
            frame_builtins: dict[str, tp.Any],
            frame_globals: dict[str, tp.Any],
            frame_locals: dict[str, tp.Any],
    ) -> None:
        self.code = frame_code
        self.builtins = frame_builtins
        self.globals = frame_globals
        self.locals = frame_locals
        self.data_stack: tp.Any = []
        self.return_value = None
        self.current_delta_counter = 0
        self.is_jump = False
        self.current_offset = 1
        self.current_argrepr = ""
        self.current_instruction: tp.Any = None
        self.current_kwargs: tp.Any = {}
        self.stop = False

    def top(self) -> tp.Any:
        return self.data_stack[-1]

    def pop(self) -> tp.Any:
        return self.data_stack.pop()

    def push(self, *values: tp.Any) -> None:
        self.data_stack.extend(values)

    def popn(self, n: int) -> tp.Any:
        """
        Pop a number of values from the value stack.
        A list of n values is returned, the deepest value first.
        """
        if n > 0:
            returned = self.data_stack[-n::]
            self.data_stack[-n:] = []
            return returned
        else:
            return []

    def run(self) -> tp.Any:
        instructions = list(dis.get_instructions(self.code))
        i = 0
        while i < len(instructions):
            instruction = instructions[i]
            self.current_instruction = instruction
            self.current_offset = instruction.offset
            self.current_argrepr = instruction.argrepr
            getattr(self, instruction.opname.lower() + "_op")(instruction.argval)
            i += 1
            if self.is_jump:
                while instructions[i].offset < self.current_delta_counter:
                    i += 1
                while instructions[i].offset > self.current_delta_counter:
                    i -= 1
                self.is_jump = False
            if self.stop:
                break
        return self.return_value

    def resume_op(self, arg: int) -> tp.Any:
        pass

    def nop_op(self, _: int) -> tp.Any:
        pass

    def push_null_op(self, _: int) -> tp.Any:
        self.push(None)

    def precall_op(self, arg: int) -> tp.Any:
        pass

    def call_op(self, arg: int) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-CALL
        """
        kwargs = {}
        if len(self.current_kwargs) > 0:
            kwargs = dict(zip(self.current_kwargs, self.popn(len(self.current_kwargs))))
            self.current_kwargs = {}
        arguments = self.popn(arg - len(kwargs))
        f = self.pop()
        if self.top() is not None:
            callables = self.pop()
            f, callables = callables, f
            self.push(f(callables, *arguments, **kwargs))
        else:
            self.pop()
            self.push(f(*arguments, **kwargs))

    def load_name_op(self, arg: str) -> None:
        """
        Partial realization

        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-LOAD_NAME
        """
        # TODO: parse all scopes

        if arg in self.locals:
            self.push(self.locals[arg])
            return
        if arg in self.globals:
            self.push(self.globals[arg])
            return
        if arg in self.builtins:
            self.push(self.builtins[arg])
            return
        raise NameError()

    def load_global_op(self, arg: str) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-LOAD_GLOBAL
        """
        # TODO: parse all scopes
        if self.current_instruction.arg & 0x1:
            self.push(None)
        if arg in self.globals:
            self.push(self.globals[arg])
            return
        if arg in self.builtins:
            self.push(self.builtins[arg])
            return
        raise NameError

    def load_const_op(self, arg: tp.Any) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-LOAD_CONST
        """
        self.push(arg)

    def load_fast_op(self, arg: str) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html
        """
        self.push(self.locals[arg])

    def load_fast_and_clear_op(self, arg: str) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html
        """
        try:
            self.push(self.locals[arg])
        except KeyError:
            self.push(None)
        self.locals[arg] = None

    def delete_fast_op(self, arg: str) -> None:
        del self.locals[arg]

    def load_fast_check_op(self, arg: str) -> None:
        if arg in self.locals:
            del self.locals[arg]
            return
        raise UnboundLocalError()

    def store_fast_op(self, arg: str) -> None:
        self.locals[arg] = self.pop()

    def return_value_op(self, _: tp.Any) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-RETURN_VALUE
        """
        self.return_value = self.pop()
        self.stop = True

    def return_const_op(self, arg: tp.Any) -> None:
        self.return_value = arg
        self.stop = True

    def pop_top_op(self, _: tp.Any) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-POP_TOP
        """
        self.pop()

    def make_function_op(self, flags: int) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-MAKE_FUNCTION
        """
        code = self.pop()  # the code associated with the function (at TOS1)
        defaults = None
        if flags & 0x01:
            defaults = self.pop()

        # TODO: use arg to parse function defaults

        def f(*args: tp.Any, **kwargs: tp.Any) -> tp.Any:
            # TODO: parse input arguments using code attributes such as co_argcount
            nonlocal defaults
            arg_names = code.co_varnames[: code.co_argcount]
            kwonly_arg_names = code.co_varnames[
                               code.co_argcount: code.co_argcount + code.co_kwonlyargcount
                               ]
            posonly_arg_count = code.co_posonlyargcount
            posonly_kwarg_count = code.co_kwonlyargcount
            args_flag = bool(code.co_flags & 4)
            kwargs_flag = bool(code.co_flags & 8)
            bound_args: dict[str, tp.Any] = {}
            defaults = defaults or ()
            kwdefaults: dict[str, tp.Any] = {}
            posonly_names = arg_names[:posonly_arg_count]

            # Positional args
            if len(arg_names) < len(args) and not args_flag:
                raise TypeError(ERR_TOO_MANY_POS_ARGS)
            else:
                for i, name in enumerate(arg_names[: len(args)]):
                    bound_args[name] = args[i]
                if args_flag:
                    bound_args["args"] = args[len(arg_names):]

            # ?
            if kwargs_flag:
                bound_args["kwargs"] = {}

            # Keyword args
            for name, value in kwargs.items():
                if name in posonly_names and not kwargs_flag:
                    raise TypeError(ERR_POSONLY_PASSED_AS_KW)
                elif name in posonly_names:
                    bound_args["kwargs"] = {name: value}
                    continue
                if name in kwonly_arg_names or name in arg_names:
                    if name in bound_args:
                        raise TypeError(ERR_MULT_VALUES_FOR_ARG)
                    bound_args[name] = value
                elif not kwargs_flag:
                    raise TypeError(ERR_TOO_MANY_KW_ARGS)
                else:
                    bound_args["kwargs"][name] = value

            # Bind default positional
            for i, name in enumerate(arg_names[-len(defaults):]):
                if name in bound_args or not defaults:
                    continue
                else:
                    bound_args[name] = defaults[i]
            for name in arg_names:
                if name not in bound_args:
                    raise TypeError(ERR_MISSING_POS_ARGS)

            # Bind default keyword args
            for name, value in kwdefaults.items():
                if name in bound_args or not kwdefaults:
                    continue
                else:
                    bound_args[name] = value
            for name in kwonly_arg_names:
                if name not in bound_args:
                    raise TypeError(ERR_MISSING_KWONLY_ARGS)

            nonlocal_names = code.co_varnames[
                               : code.co_argcount
                               + posonly_arg_count
                               + posonly_kwarg_count
                               + kwargs_flag
                               + args_flag
                             ]

            if kwargs_flag:
                tmp = bound_args["kwargs"]
                del bound_args["kwargs"]
                bound_args[nonlocal_names[-1]] = tmp
            if args_flag:
                tmp = bound_args["args"]
                del bound_args["args"]
                bound_args[
                    nonlocal_names[-2] if kwargs_flag else nonlocal_names[-1]
                ] = tmp

            # todo:
            parsed_args: dict[str, tp.Any] = bound_args
            f_locals = dict(self.locals)
            f_locals.update(parsed_args)

            frame = Frame(
                code, self.builtins, self.globals, f_locals
            )  # Run code in prepared environment
            return frame.run()

        if flags & 0x04:
            data = self.pop()
            f.__annotations__ = {data[i]: data[i + 1] for i in range(0, len(data), 2)}
        self.push(f)

    def store_name_op(self, arg: str) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-STORE_NAME
        """
        const = self.pop()
        self.locals[arg] = const

    def store_global_op(self, arg: str) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-STORE_NAME
        """
        const = self.pop()
        self.builtins[arg] = const

    def binary_op_op(self, _: tp.Any) -> None:
        lhs, rhs = self.popn(2)
        self.push(BINARY_OPS[self.current_argrepr](lhs, rhs))

    def unary_negative_op(self, _: tp.Any) -> None:
        self.push(-self.pop())

    def unary_not_op(self, _: tp.Any) -> None:
        self.push(not self.pop())

    def unary_invert_op(self, _: tp.Any) -> None:
        self.push(~self.pop())

    def call_intrinsic_1_op(self, num: tp.Any) -> None:
        if num == 2:
            module = self.top()
            for attr in dir(module):
                if not attr.startswith("_"):
                    self.locals[attr] = getattr(module, attr)
        elif num == 5:
            pass
        elif num == 6:
            self.push(tuple(self.pop()))

    def compare_op_op(self, op: str) -> None:
        lhs, rhs = self.popn(2)
        self.push(COMPARE_OPS[op](lhs, rhs))

    def contains_op_op(self, invert: int) -> None:
        lhs, rhs = self.popn(2)
        self.push(bool((lhs in rhs) ^ invert))

    def is_op_op(self, invert: int) -> None:
        lhs, rhs = self.popn(2)
        self.push(bool((lhs is rhs) ^ invert))

    def pop_jump_if_true_op(self, delta: int) -> None:
        if self.pop():
            self.current_delta_counter = delta
            self.is_jump = True

    def pop_jump_if_false_op(self, delta: int) -> None:
        if not self.pop():
            self.current_delta_counter = delta
            self.is_jump = True

    def pop_jump_if_none_op(self, delta: int) -> None:
        if self.pop() is None:
            self.current_delta_counter = delta
            self.is_jump = True

    def pop_jump_if_not_none_op(self, delta: int) -> None:
        if self.pop() is not None:
            self.current_delta_counter = delta
            self.is_jump = True

    def jump_forward_op(self, delta: int) -> None:
        self.current_delta_counter = delta
        self.is_jump = True

    def jump_backward_op(self, delta: int) -> None:
        self.current_delta_counter = delta
        self.is_jump = True

    def unpack_sequence_op(self, count: int) -> None:
        container = self.pop()
        self.push(*container[: -count - 1: -1])

    def binary_slice_op(self, _: tp.Any) -> None:
        end = self.pop()
        start = self.pop()
        container = self.pop()
        self.push(container[start:end])

    def build_slice_op(self, arg: int) -> None:
        if arg == 3:
            step = self.pop()
            end = self.pop()
            start = self.pop()
            self.push(slice(start, end, step))
        else:
            end = self.pop()
            start = self.pop()
            self.push(slice(start, end))

    def binary_subscr_op(self, _: tp.Any) -> None:
        key = self.pop()
        container = self.pop()
        self.push(container[key])

    def delete_subscr_op(self, _: tp.Any) -> None:
        key = self.pop()
        container = self.pop()
        del container[key]

    def build_tuple_op(self, count: int) -> None:
        if count == 0:
            value = ()
        else:
            values = self.popn(count)
            value = tuple(values)

        self.push(value)

    def build_list_op(self, count: int) -> None:
        if count == 0:
            value = []
        else:
            values = self.popn(count)
            value = values

        self.push(value)

    def list_extend_op(self, _: tp.Any) -> None:
        seq = self.pop()
        list.extend(self.top(), seq)

    def list_append_op(self, i: int) -> None:
        item = self.pop()
        list.append(self.data_stack[-i], item)

    def store_slice_op(self, _: tp.Any) -> None:
        end = self.pop()
        start = self.pop()
        container = self.pop()
        values = self.pop()
        container[start:end] = values

    def get_iter_op(self, _: tp.Any) -> None:
        top = self.pop()
        self.push(iter(top))

    def for_iter_op(self, delta: int) -> None:
        try:
            it = next(self.top())
            self.push(it)
        except StopIteration:
            self.is_jump = True
            self.current_delta_counter = delta

    def end_for_op(self, _: tp.Any) -> None:
        self.popn(1)

    def build_map_op(self, count: int) -> None:
        values = self.popn(2 * count)
        self.push({values[i]: values[i + 1] for i in range(0, 2 * count, 2)})

    def dict_update_op(self, _: tp.Any) -> None:
        seq = self.pop()
        dict.update(self.top(), seq)

    def dict_merge_op(self, _: tp.Any) -> None:
        seq = self.pop()
        a = set(self.top().keys())
        b = set(seq.keys())
        if len(a.intersection(b)) > 0:
            raise ValueError
        dict.update(self.top(), seq)

    def build_const_key_map_op(self, count: int) -> None:
        keys = self.pop()
        values = self.popn(count)
        self.push({keys[i]: values[i] for i in range(count)})

    def map_add_op(self, i: int) -> None:
        value = self.pop()
        key = self.pop()
        dict.__setitem__(self.data_stack[-i], key, value)

    def build_set_op(self, count: int) -> None:
        if count == 0:
            value = set()
        else:
            values = self.popn(count)
            value = set(values)
        self.push(value)

    def set_update_op(self, _: tp.Any) -> None:
        seq = self.pop()
        set.update(self.top(), seq)

    def set_add_op(self, i: int) -> None:
        item = self.pop()
        set.add(self.data_stack[-i], item)

    def kw_names_op(self, arg: str) -> None:
        self.current_kwargs = arg

    def swap_op(self, i: int) -> None:
        self.data_stack[-i], self.data_stack[-1] = (
            self.data_stack[-1],
            self.data_stack[-i],
        )

    def format_value_op(self, flags: tuple[tp.Any, bool]) -> None:
        fmt_spec = None
        if flags[1]:
            fmt_spec = self.pop()
        value = self.pop()
        if flags[0] is not None:
            value = flags[0](value)
        self.push(format(value if fmt_spec is None else format(value, fmt_spec)))

    def build_string_op(self, count: int) -> None:
        if count == 0:
            value = ""
        else:
            values = self.popn(count)
            value = "".join(values)
        self.push(value)

    def store_subscr_op(self, _: tp.Any) -> None:
        key = self.pop()
        container = self.pop()
        value = self.pop()
        container[key] = value

    def copy_op(self, i: int) -> None:
        self.push(self.data_stack[-i])

    def load_attr_op(self, arg: str) -> None:
        if not self.current_instruction.arg & 0x1:
            obj = self.pop()
            self.push(getattr(obj, arg))
        else:
            obj = self.pop()
            self.push(None)
            self.push(getattr(obj, arg))

    def store_attr_op(self, arg: str) -> None:
        obj = self.pop()
        value = self.pop()
        setattr(obj, arg, value)

    def call_function_ex_op(self, arg: int) -> None:
        kwargs = {}
        if arg & 0x01:
            kwargs = self.pop()
        posargs = self.pop()
        func = self.pop()
        self.push(func(*posargs, **kwargs))

    def setup_annotations_op(self, _: int) -> None:
        if "__annotations__" not in self.locals:
            self.locals["__annotations__"] = {}

    def delete_attr_op(self, arg: str) -> None:
        obj = self.pop()
        delattr(obj, arg)

    def delete_name_op(self, arg: str) -> None:
        del self.locals[arg]

    def delete_global_op(self, arg: str) -> None:
        del self.builtins[arg]

    def import_name_op(self, arg: str) -> None:
        self.push(__import__(arg, fromlist=self.pop(), level=self.pop()))

    def import_from_op(self, arg: str) -> None:
        self.locals[arg] = getattr(self.top(), arg)
        self.push(getattr(self.top(), arg))

    def load_assertion_error_op(self, _: tp.Any) -> None:
        self.push(AssertionError)

    def raise_varargs_op(self, arg: tp.Any) -> None:
        if arg == 0:
            raise
        elif arg == 1:
            raise self.pop()
        elif arg == 2:
            a, b = self.popn(2)
            raise b from b

    def load_build_class_op(self, _: tp.Any) -> None:
        self.push(__build_class__)

    def extended_arg_op(self, _: tp.Any) -> None:
        pass

    def unpack_ex_op(self, count: tp.Any) -> None:
        pass


class VirtualMachine:
    @staticmethod
    def run(code_obj: types.CodeType) -> None:
        """
        :param code_obj: code for interpreting
        """
        globals_context: dict[str, tp.Any] = {}
        frame = Frame(
            code_obj,
            builtins.globals()["__builtins__"],
            globals_context,
            globals_context,
        )
        return frame.run()

# todo: 27
# todo: 50
# todo: 60
# todo: 76
# todo: 78
# todo: 90
# todo: 93
# todo: 106
# todo: 113
# todo: 116
# todo: 124
# todo: 129
# todo: 130
# todo: 141
# todo: 145
# todo: 154
# todo: 158
# todo: 171
# todo: 187
# todo: 190
# todo: 218
# todo: 221
# todo: 225
# todo: 229
# todo: 230
# todo: 238
# todo: 242
# todo: 246
# todo: 253
# todo: 255
