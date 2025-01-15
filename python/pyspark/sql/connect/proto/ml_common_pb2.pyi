#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import builtins
import collections.abc
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import pyspark.sql.connect.proto.expressions_pb2
import sys
import typing

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class MlParams(google.protobuf.message.Message):
    """MlParams stores param settings for ML Estimator / Transformer / Evaluator"""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class ParamsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.str
        @property
        def value(self) -> global___Param: ...
        def __init__(
            self,
            *,
            key: builtins.str = ...,
            value: global___Param | None = ...,
        ) -> None: ...
        def HasField(
            self, field_name: typing_extensions.Literal["value", b"value"]
        ) -> builtins.bool: ...
        def ClearField(
            self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]
        ) -> None: ...

    PARAMS_FIELD_NUMBER: builtins.int
    @property
    def params(
        self,
    ) -> google.protobuf.internal.containers.MessageMap[builtins.str, global___Param]:
        """User-supplied params"""
    def __init__(
        self,
        *,
        params: collections.abc.Mapping[builtins.str, global___Param] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["params", b"params"]) -> None: ...

global___MlParams = MlParams

class Param(google.protobuf.message.Message):
    """Represents the parameter type of the ML instance, or the returned value
    of the attribute
    """

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    LITERAL_FIELD_NUMBER: builtins.int
    VECTOR_FIELD_NUMBER: builtins.int
    MATRIX_FIELD_NUMBER: builtins.int
    @property
    def literal(self) -> pyspark.sql.connect.proto.expressions_pb2.Expression.Literal: ...
    @property
    def vector(self) -> global___Vector: ...
    @property
    def matrix(self) -> global___Matrix: ...
    def __init__(
        self,
        *,
        literal: pyspark.sql.connect.proto.expressions_pb2.Expression.Literal | None = ...,
        vector: global___Vector | None = ...,
        matrix: global___Matrix | None = ...,
    ) -> None: ...
    def HasField(
        self,
        field_name: typing_extensions.Literal[
            "literal",
            b"literal",
            "matrix",
            b"matrix",
            "param_type",
            b"param_type",
            "vector",
            b"vector",
        ],
    ) -> builtins.bool: ...
    def ClearField(
        self,
        field_name: typing_extensions.Literal[
            "literal",
            b"literal",
            "matrix",
            b"matrix",
            "param_type",
            b"param_type",
            "vector",
            b"vector",
        ],
    ) -> None: ...
    def WhichOneof(
        self, oneof_group: typing_extensions.Literal["param_type", b"param_type"]
    ) -> typing_extensions.Literal["literal", "vector", "matrix"] | None: ...

global___Param = Param

class MlOperator(google.protobuf.message.Message):
    """MLOperator represents the ML operators like (Estimator, Transformer or Evaluator)"""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class _OperatorType:
        ValueType = typing.NewType("ValueType", builtins.int)
        V: typing_extensions.TypeAlias = ValueType

    class _OperatorTypeEnumTypeWrapper(
        google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[
            MlOperator._OperatorType.ValueType
        ],
        builtins.type,
    ):  # noqa: F821
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        UNSPECIFIED: MlOperator._OperatorType.ValueType  # 0
        ESTIMATOR: MlOperator._OperatorType.ValueType  # 1
        TRANSFORMER: MlOperator._OperatorType.ValueType  # 2
        EVALUATOR: MlOperator._OperatorType.ValueType  # 3
        MODEL: MlOperator._OperatorType.ValueType  # 4

    class OperatorType(_OperatorType, metaclass=_OperatorTypeEnumTypeWrapper): ...
    UNSPECIFIED: MlOperator.OperatorType.ValueType  # 0
    ESTIMATOR: MlOperator.OperatorType.ValueType  # 1
    TRANSFORMER: MlOperator.OperatorType.ValueType  # 2
    EVALUATOR: MlOperator.OperatorType.ValueType  # 3
    MODEL: MlOperator.OperatorType.ValueType  # 4

    NAME_FIELD_NUMBER: builtins.int
    UID_FIELD_NUMBER: builtins.int
    TYPE_FIELD_NUMBER: builtins.int
    name: builtins.str
    """The qualified name of the ML operator."""
    uid: builtins.str
    """Unique id of the ML operator"""
    type: global___MlOperator.OperatorType.ValueType
    """Represents what the ML operator is"""
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        uid: builtins.str = ...,
        type: global___MlOperator.OperatorType.ValueType = ...,
    ) -> None: ...
    def ClearField(
        self, field_name: typing_extensions.Literal["name", b"name", "type", b"type", "uid", b"uid"]
    ) -> None: ...

global___MlOperator = MlOperator

class ObjectRef(google.protobuf.message.Message):
    """Represents a reference to the cached object which could be a model
    or summary evaluated by a model
    """

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ID_FIELD_NUMBER: builtins.int
    id: builtins.str
    """The ID is used to lookup the object on the server side."""
    def __init__(
        self,
        *,
        id: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["id", b"id"]) -> None: ...

global___ObjectRef = ObjectRef

class Vector(google.protobuf.message.Message):
    """See pyspark.ml.linalg.Vector"""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class Dense(google.protobuf.message.Message):
        """See pyspark.ml.linalg.DenseVector"""

        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        VALUE_FIELD_NUMBER: builtins.int
        @property
        def value(
            self,
        ) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]: ...
        def __init__(
            self,
            *,
            value: collections.abc.Iterable[builtins.float] | None = ...,
        ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["value", b"value"]) -> None: ...

    class Sparse(google.protobuf.message.Message):
        """See pyspark.ml.linalg.SparseVector"""

        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        SIZE_FIELD_NUMBER: builtins.int
        INDEX_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        size: builtins.int
        @property
        def index(
            self,
        ) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.int]: ...
        @property
        def value(
            self,
        ) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]: ...
        def __init__(
            self,
            *,
            size: builtins.int = ...,
            index: collections.abc.Iterable[builtins.int] | None = ...,
            value: collections.abc.Iterable[builtins.float] | None = ...,
        ) -> None: ...
        def ClearField(
            self,
            field_name: typing_extensions.Literal[
                "index", b"index", "size", b"size", "value", b"value"
            ],
        ) -> None: ...

    DENSE_FIELD_NUMBER: builtins.int
    SPARSE_FIELD_NUMBER: builtins.int
    @property
    def dense(self) -> global___Vector.Dense: ...
    @property
    def sparse(self) -> global___Vector.Sparse: ...
    def __init__(
        self,
        *,
        dense: global___Vector.Dense | None = ...,
        sparse: global___Vector.Sparse | None = ...,
    ) -> None: ...
    def HasField(
        self,
        field_name: typing_extensions.Literal[
            "dense", b"dense", "sparse", b"sparse", "vector_type", b"vector_type"
        ],
    ) -> builtins.bool: ...
    def ClearField(
        self,
        field_name: typing_extensions.Literal[
            "dense", b"dense", "sparse", b"sparse", "vector_type", b"vector_type"
        ],
    ) -> None: ...
    def WhichOneof(
        self, oneof_group: typing_extensions.Literal["vector_type", b"vector_type"]
    ) -> typing_extensions.Literal["dense", "sparse"] | None: ...

global___Vector = Vector

class Matrix(google.protobuf.message.Message):
    """See pyspark.ml.linalg.Matrix"""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class Dense(google.protobuf.message.Message):
        """See pyspark.ml.linalg.DenseMatrix"""

        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        NUM_ROWS_FIELD_NUMBER: builtins.int
        NUM_COLS_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        IS_TRANSPOSED_FIELD_NUMBER: builtins.int
        num_rows: builtins.int
        num_cols: builtins.int
        @property
        def value(
            self,
        ) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]: ...
        is_transposed: builtins.bool
        def __init__(
            self,
            *,
            num_rows: builtins.int = ...,
            num_cols: builtins.int = ...,
            value: collections.abc.Iterable[builtins.float] | None = ...,
            is_transposed: builtins.bool = ...,
        ) -> None: ...
        def ClearField(
            self,
            field_name: typing_extensions.Literal[
                "is_transposed",
                b"is_transposed",
                "num_cols",
                b"num_cols",
                "num_rows",
                b"num_rows",
                "value",
                b"value",
            ],
        ) -> None: ...

    class Sparse(google.protobuf.message.Message):
        """See pyspark.ml.linalg.SparseMatrix"""

        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        NUM_ROWS_FIELD_NUMBER: builtins.int
        NUM_COLS_FIELD_NUMBER: builtins.int
        COLPTR_FIELD_NUMBER: builtins.int
        ROW_INDEX_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        IS_TRANSPOSED_FIELD_NUMBER: builtins.int
        num_rows: builtins.int
        num_cols: builtins.int
        @property
        def colptr(
            self,
        ) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.int]: ...
        @property
        def row_index(
            self,
        ) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.int]: ...
        @property
        def value(
            self,
        ) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]: ...
        is_transposed: builtins.bool
        def __init__(
            self,
            *,
            num_rows: builtins.int = ...,
            num_cols: builtins.int = ...,
            colptr: collections.abc.Iterable[builtins.int] | None = ...,
            row_index: collections.abc.Iterable[builtins.int] | None = ...,
            value: collections.abc.Iterable[builtins.float] | None = ...,
            is_transposed: builtins.bool = ...,
        ) -> None: ...
        def ClearField(
            self,
            field_name: typing_extensions.Literal[
                "colptr",
                b"colptr",
                "is_transposed",
                b"is_transposed",
                "num_cols",
                b"num_cols",
                "num_rows",
                b"num_rows",
                "row_index",
                b"row_index",
                "value",
                b"value",
            ],
        ) -> None: ...

    DENSE_FIELD_NUMBER: builtins.int
    SPARSE_FIELD_NUMBER: builtins.int
    @property
    def dense(self) -> global___Matrix.Dense: ...
    @property
    def sparse(self) -> global___Matrix.Sparse: ...
    def __init__(
        self,
        *,
        dense: global___Matrix.Dense | None = ...,
        sparse: global___Matrix.Sparse | None = ...,
    ) -> None: ...
    def HasField(
        self,
        field_name: typing_extensions.Literal[
            "dense", b"dense", "matrix_type", b"matrix_type", "sparse", b"sparse"
        ],
    ) -> builtins.bool: ...
    def ClearField(
        self,
        field_name: typing_extensions.Literal[
            "dense", b"dense", "matrix_type", b"matrix_type", "sparse", b"sparse"
        ],
    ) -> None: ...
    def WhichOneof(
        self, oneof_group: typing_extensions.Literal["matrix_type", b"matrix_type"]
    ) -> typing_extensions.Literal["dense", "sparse"] | None: ...

global___Matrix = Matrix