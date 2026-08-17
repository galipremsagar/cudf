# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Deferred joins, so a filter written after a join can run before it.

Analytical queries are routinely written join-first-filter-after and left to an
optimizer to reorder -- PDS-H does exactly this, and says so. Executing that
literally is what breaks at scale: at SF300, q10's ``customer.merge(orders)``
materializes 81.5 GiB and the next line joins the full 1.8-billion-row lineitem
to it, where filtering first makes the same query peak at 231 GiB.

So ``merge`` returns a :class:`LazyJoin` that records the plan and does nothing.
Selecting a column from it yields an :class:`Expr` -- a deferred expression that
remembers which *input* its columns came from. Using an Expr as a mask rewrites
the plan to filter that input instead of the output.

The critical property is that an Expr can be evaluated against **any** frame,
not just the input it was built from. When a predicate can be pushed, it is
evaluated against the input; when it cannot -- because it spans both inputs, or
would filter the null-extended side of an outer join -- it is evaluated against
the materialized join instead, and the answer is identical to never having been
lazy. Laziness here only ever changes *when* work happens.
"""

from __future__ import annotations

from typing import Any, Callable

from ._frame import ChunkedDataFrame, ChunkedFrame, ChunkedSeries, unwrap_proxy

__all__ = ["JoinPlan", "Expr"]

#: Which side of a join may receive a pushed predicate.
#: Pushing into the null-extended side of an outer join is wrong: rows the
#: predicate removes would have come back as null-extended rows, so dropping
#: them early changes the answer.
_PUSHABLE = {
    "inner": ("left", "right"),
    "left": ("left",),
    "right": ("right",),
    "outer": (),
    "cross": (),
    "leftsemi": ("left",),
    "leftanti": ("left",),
}

#: (dunder, whether the right operand is the deferred one)
_BINARY_OPS = [
    "add", "sub", "mul", "truediv", "floordiv", "mod", "pow",
    "and", "or", "xor", "lt", "le", "gt", "ge", "eq", "ne",
]


class Expr:
    """A deferred expression over the columns of a pending join.

    ``origin`` is the single input frame every referenced column came from, or
    ``None`` once an operation mixes two inputs -- which is exactly the case
    where the expression cannot be evaluated by either side alone.
    """

    __slots__ = ("_owner", "_origin", "_eval", "_deferred_call")

    def __init__(self, owner: "LazyJoin", origin: Any,
                 evaluate: Callable[[Any], Any]) -> None:
        self._owner = owner
        self._origin = origin
        self._eval = evaluate

    # -- composition --------------------------------------------------
    def _derive(self, other, op: str, reflected: bool = False) -> "Expr":
        other = unwrap_proxy(other)
        mine = self._eval
        if isinstance(other, Expr):
            origin = self._origin if other._origin is self._origin else None
            theirs = other._eval
            if reflected:
                return Expr(self._owner, origin,
                            lambda f: getattr(theirs(f), op)(mine(f)))
            return Expr(self._owner, origin,
                        lambda f: getattr(mine(f), op)(theirs(f)))
        if isinstance(other, ChunkedFrame):
            # a concrete frame cannot be re-evaluated against another frame,
            # so this expression is no longer portable
            origin = None
            if reflected:
                return Expr(self._owner, origin,
                            lambda f: getattr(other, op)(mine(f)))
            return Expr(self._owner, origin,
                        lambda f: getattr(mine(f), op)(other))
        if reflected:
            return Expr(self._owner, self._origin,
                        lambda f: getattr(mine(f), _reflect(op))(other))
        return Expr(self._owner, self._origin,
                    lambda f: getattr(mine(f), op)(other))

    def __invert__(self) -> "Expr":
        mine = self._eval
        return Expr(self._owner, self._origin, lambda f: ~mine(f))

    def __neg__(self) -> "Expr":
        mine = self._eval
        return Expr(self._owner, self._origin, lambda f: -mine(f))

    def isin(self, values) -> "Expr":
        mine = self._eval
        return Expr(self._owner, self._origin, lambda f: mine(f).isin(values))

    def astype(self, dtype) -> "Expr":
        mine = self._eval
        return Expr(self._owner, self._origin,
                    lambda f: mine(f).astype(dtype))

    @property
    def str(self):
        return _ExprAccessor(self, "str")

    @property
    def dt(self):
        return _ExprAccessor(self, "dt")

    # -- evaluation ---------------------------------------------------
    def evaluate(self, frame) -> Any:
        return self._eval(frame)

    def materialize(self) -> Any:
        """Evaluate against the executed join -- always correct, never lazy."""
        owner = self._owner
        if getattr(owner, "_plan", None) is not None:
            owner._materialize()
        return self._eval(owner)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self.materialize(), name)

    def __len__(self):
        return len(self.materialize())

    def __iter__(self):
        return iter(self.materialize())

    def __bool__(self):
        raise ValueError(
            "the truth value of a deferred column expression is ambiguous; "
            "use it as a mask or call .materialize()"
        )

    def __repr__(self) -> str:
        return (f"<Expr over a pending join, origin="
                f"{'mixed' if self._origin is None else 'one input'}>")


def _reflect(op: str) -> str:
    return f"__r{op[2:-2]}__" if not op.startswith("__r") else op


def _install_ops():
    for name in _BINARY_OPS:
        op = f"__{name}__"

        def forward(self, other, _op=op):
            return self._derive(other, _op)

        def reverse(self, other, _op=op):
            return self._derive(other, _op, reflected=True)

        forward.__name__ = op
        setattr(Expr, op, forward)
        if name not in ("lt", "le", "gt", "ge", "eq", "ne"):
            rop = f"__r{name}__"
            reverse.__name__ = rop
            setattr(Expr, rop, reverse)


_install_ops()


class _DeferredAttr(Expr):
    """``expr.dt.year`` and ``expr.str.upper()`` are both this.

    An accessor member may be a value (``.dt.year``) or a method
    (``.str.startswith``), and which one it is depends on the column's dtype --
    which is exactly what a pending join has not computed yet. So this behaves
    as the attribute *and* stays callable: use it directly and it evaluates the
    attribute, call it and it evaluates the call.
    """

    __slots__ = ()

    def __init__(self, owner, origin, inner, namespace, name) -> None:
        super().__init__(
            owner, origin,
            lambda f: getattr(getattr(inner(f), namespace), name),
        )
        object.__setattr__(self, "_deferred_call",
                           (owner, origin, inner, namespace, name))

    def __call__(self, *args, **kwargs) -> Expr:
        owner, origin, inner, namespace, name = self._deferred_call
        return Expr(
            owner, origin,
            lambda f: getattr(getattr(inner(f), namespace), name)(
                *args, **kwargs),
        )


class _ExprAccessor:
    """``expr.str`` / ``expr.dt`` over a pending join."""

    def __init__(self, expr: Expr, namespace: str) -> None:
        self._expr = expr
        self._namespace = namespace

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        expr = self._expr
        return _DeferredAttr(expr._owner, expr._origin, expr._eval,
                             self._namespace, name)


_MISS = object()


class JoinPlan:
    """A merge that has been recorded but not run.

    Held by an ordinary :class:`ChunkedDataFrame` (see ``ChunkedFrame._plan``)
    rather than by a subclass, so the frame's exact type is unchanged and
    cudf.pandas keeps wrapping results.
    """

    __slots__ = ("left", "right", "kwargs", "runtime")

    def __init__(self, left, right, kwargs: dict) -> None:
        # Snapshot the inputs. pandas' merge is eager, so its result cannot
        # change when an input is written to afterwards; holding the caller's
        # frame objects made `out = a.merge(b); a["x"] = -1; out` join the
        # *mutated* a. _shallow_copy moves no device data, and because chunk
        # mutation is copy-on-write (see ChunkedDataFrame.__setitem__), a
        # later write to `a` rebinds a's chunk list and leaves this one alone.
        self.left = unwrap_proxy(left)._shallow_copy()
        self.right = unwrap_proxy(right)._shallow_copy()
        self.kwargs = kwargs
        self.runtime = self.left.runtime

    # -- execution -----------------------------------------------------
    def execute(self) -> ChunkedDataFrame:
        from ._ops import execute_merge

        return execute_merge(self.left, self.right, self.kwargs)

    def meta(self):
        kwargs = {k: v for k, v in self.kwargs.items()
                  if k not in ("broadcast", "nparts")}
        return self.left._meta.merge(self.right._meta, **kwargs)

    # -- pushdown ------------------------------------------------------
    def _side_of(self, name):
        """Which input a column unambiguously came from, or None.

        A name on both inputs is renamed by merge's suffixes, so the bare name
        no longer identifies a side; a name on neither is not a column at all.
        Both cases refuse to attribute an origin.
        """
        in_left = name in self.left._meta.columns
        in_right = name in self.right._meta.columns
        if in_left and not in_right:
            return self.left
        if in_right and not in_left:
            return self.right
        return None

    def _pushable(self) -> list:
        allowed = _PUSHABLE.get(self.kwargs.get("how", "inner"), ())
        sides = []
        if "left" in allowed:
            sides.append(self.left)
        if "right" in allowed:
            sides.append(self.right)
        return sides

    def push(self, expr: "Expr"):
        """A re-planned join with the predicate applied to one input, or None."""
        origin = expr._origin
        if origin is None:
            return None  # spans both inputs; neither can evaluate it alone
        if not any(origin is side for side in self._pushable()):
            return None  # would filter the null-extended side of an outer join
        filtered = origin[expr.evaluate(origin)]
        left = filtered if origin is self.left else self.left
        right = filtered if origin is self.right else self.right
        return ChunkedDataFrame(plan=JoinPlan(left, right, self.kwargs))

    # -- what a pending frame answers without executing -----------------
    def getitem(self, frame, key):
        key = unwrap_proxy(key)
        if isinstance(key, Expr):
            pushed = self.push(key)
            if pushed is not None:
                return pushed
            frame._materialize()
            return frame[key.evaluate(frame)]
        if isinstance(key, (str, int)):
            source = self._side_of(key)
            if source is not None:
                return Expr(frame, source, lambda f, n=key: f[n])
        return _MISS
