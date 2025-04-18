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
from typing import Any, Dict, Iterator, List, Union, Tuple

from pyspark.sql.streaming.stateful_processor_api_client import StatefulProcessorApiClient
from pyspark.sql.types import StructType
from pyspark.errors import PySparkRuntimeError
import uuid

__all__ = ["ListStateClient"]


class ListStateClient:
    def __init__(
        self,
        stateful_processor_api_client: StatefulProcessorApiClient,
        schema: Union[StructType, str],
    ) -> None:
        self._stateful_processor_api_client = stateful_processor_api_client
        if isinstance(schema, str):
            self.schema = self._stateful_processor_api_client._parse_string_schema(schema)
        else:
            self.schema = schema
        # A dictionary to store the mapping between list state name and a tuple of data batch
        # and the index of the last row that was read.
        self.data_batch_dict: Dict[str, Tuple[Any, int]] = {}

    def exists(self, state_name: str) -> bool:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        exists_call = stateMessage.Exists()
        list_state_call = stateMessage.ListStateCall(stateName=state_name, exists=exists_call)
        state_variable_request = stateMessage.StateVariableRequest(listStateCall=list_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status == 0:
            return True
        elif status == 2:
            # Expect status code is 2 when state variable doesn't have a value.
            return False
        else:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(
                f"Error checking value state exists: " f"{response_message[1]}"
            )

    def get(self, state_name: str, iterator_id: str) -> Tuple:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        if iterator_id in self.data_batch_dict:
            # If the state is already in the dictionary, return the next row.
            data_batch, index = self.data_batch_dict[iterator_id]
        else:
            # If the state is not in the dictionary, fetch the state from the server.
            get_call = stateMessage.ListStateGet(iteratorId=iterator_id)
            list_state_call = stateMessage.ListStateCall(
                stateName=state_name, listStateGet=get_call
            )
            state_variable_request = stateMessage.StateVariableRequest(
                listStateCall=list_state_call
            )
            message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

            self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
            response_message = self._stateful_processor_api_client._receive_proto_message()
            status = response_message[0]
            if status == 0:
                data_batch = self._stateful_processor_api_client._read_list_state()
                index = 0
            else:
                raise StopIteration()

        new_index = index + 1
        if new_index < len(data_batch):
            # Update the index in the dictionary.
            self.data_batch_dict[iterator_id] = (data_batch, new_index)
        else:
            # If the index is at the end of the data batch, remove the state from the dictionary.
            self.data_batch_dict.pop(iterator_id, None)
        row = data_batch[index]
        return tuple(row)

    def append_value(self, state_name: str, value: Tuple) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        bytes = self._stateful_processor_api_client._serialize_to_bytes(self.schema, value)
        append_value_call = stateMessage.AppendValue(value=bytes)
        list_state_call = stateMessage.ListStateCall(
            stateName=state_name, appendValue=append_value_call
        )
        state_variable_request = stateMessage.StateVariableRequest(listStateCall=list_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error updating value state: " f"{response_message[1]}")

    def append_list(self, state_name: str, values: List[Tuple]) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        append_list_call = stateMessage.AppendList()
        list_state_call = stateMessage.ListStateCall(
            stateName=state_name, appendList=append_list_call
        )
        state_variable_request = stateMessage.StateVariableRequest(listStateCall=list_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())

        self._stateful_processor_api_client._send_list_state(self.schema, values)
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error updating value state: " f"{response_message[1]}")

    def put(self, state_name: str, values: List[Tuple]) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        put_call = stateMessage.ListStatePut()
        list_state_call = stateMessage.ListStateCall(stateName=state_name, listStatePut=put_call)
        state_variable_request = stateMessage.StateVariableRequest(listStateCall=list_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())

        self._stateful_processor_api_client._send_list_state(self.schema, values)
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error updating value state: " f"{response_message[1]}")

    def clear(self, state_name: str) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        clear_call = stateMessage.Clear()
        list_state_call = stateMessage.ListStateCall(stateName=state_name, clear=clear_call)
        state_variable_request = stateMessage.StateVariableRequest(listStateCall=list_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error clearing value state: " f"{response_message[1]}")


class ListStateIterator:
    def __init__(self, list_state_client: ListStateClient, state_name: str):
        self.list_state_client = list_state_client
        self.state_name = state_name
        # Generate a unique identifier for the iterator to make sure iterators from the same
        # list state do not interfere with each other.
        self.iterator_id = str(uuid.uuid4())

    def __iter__(self) -> Iterator[Tuple]:
        return self

    def __next__(self) -> Tuple:
        return self.list_state_client.get(self.state_name, self.iterator_id)
