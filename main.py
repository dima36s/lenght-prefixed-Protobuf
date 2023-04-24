import re

from google.protobuf.internal.decoder import _DecodeVarint32

import message_pb2

min_mes_SIZE = 4
max_mes_SIZE = 8
RFFR_MSG_SIZE = 2
FR_MSG_SIZE = 23
FR_SKIP = 4
SR_RFSR_SKIP = 3
RFFR_SKIP = 1


class DelimitedMessagesStreamParser:
    def __init__(self):
        self.m_buffer = bytearray()
        self.parsed = []
        self.bytesConsumed = None
        pass

    def parse(self, data: bytearray) -> list:
        bufferNotChangedTimes = 0
        self.m_buffer += data
        self.bytesConsumed = None
        while len(self.m_buffer) > 0:
            new_mes, self.bytesConsumed = self.parseDelimited(self.m_buffer, len(self.m_buffer), self.bytesConsumed)
            print(self.m_buffer)
            if self.bytesConsumed is not None:
                self.m_buffer = self.m_buffer[self.bytesConsumed:]
                if new_mes is not None:
                    self.parsed.append(new_mes)
                self.bytesConsumed = None
                bufferNotChangedTimes = 0
            else:
                bufferNotChangedTimes += 1
                if bufferNotChangedTimes > (max(FR_MSG_SIZE, RFFR_MSG_SIZE, max_mes_SIZE) + 1):
                    break
        return self.parsed

    def clear(self):
        self.m_buffer.clear()
        self.parsed.clear()

    def getParsed(self):
        return self.parsed

    def parseDelimited(self, buffer: bytearray, bufSize: int, bytesConsumed=None):
        (msgSize, pos) = _DecodeVarint32(buffer, 0)
        current_message = None
        result = None
        successfullyParsed = False
        if msgSize < bufSize:
            if (msgSize >= min_mes_SIZE) and (msgSize <= max_mes_SIZE):
                current_message = buffer[pos:(msgSize + pos)]
                (connected_client_count, connected_client_count_pos) = _DecodeVarint32(current_message,
                                                                                       SR_RFSR_SKIP)
                if not isCorrectRangeVarint32(msgSize, connected_client_count):
                    bytesConsumed = msgSize + 1
                    return None, bytesConsumed
                sr = message_pb2.WrapperMessage()
                sr.slow_response.connected_client_count = connected_client_count
                result = sr
                successfullyParsed = True

            elif msgSize == RFFR_MSG_SIZE:
                current_message = buffer[pos + RFFR_SKIP:(msgSize + pos + RFFR_SKIP)]
                if current_message[0] != 0:
                    bytesConsumed = 1
                    return None, bytesConsumed
                else:
                    mes_r = message_pb2.WrapperMessage()
                    mes_r.request_for_fast_response.SetInParent()
                    result = mes_r
                    successfullyParsed = True

            elif msgSize == FR_MSG_SIZE:
                regexPattern = r"(([12][0-9]{3})(0[1-9]|1[012])(0[1-9]|[12]\d|3[01])T([0-1]?[0-9]|2[0-3])[0-5]\d[" \
                               r"0-5]\d.\d\d\d) "
                current_message = buffer[pos + FR_SKIP:(msgSize + pos)].decode('utf-8')
                if not bool(re.match(regexPattern, current_message)):
                    bytesConsumed = msgSize + 1
                    return None, bytesConsumed
                fr = message_pb2.WrapperMessage()
                fr.fast_response.current_date_time = current_message
                result = fr
                successfullyParsed = True

            if successfullyParsed:
                bytesConsumed = msgSize + 1
                return result, bytesConsumed
            else:
                bytesConsumed = 1
                return None, bytesConsumed
        else:
            if ((msgSize < min_mes_SIZE) or (msgSize > max_mes_SIZE)) \
                    and (msgSize != FR_MSG_SIZE) \
                    and (msgSize != RFFR_MSG_SIZE):
                bytesConsumed = 1
                return None, bytesConsumed
        return None, bytesConsumed


def isCorrectRangeVarint32(msgSize, result) -> bool:
    if msgSize == 4:
        if result >= pow(2, 7):
            return False
    elif msgSize == 5:
        if (result < pow(2, 7)) or (result >= pow(2, 14)):
            return False
    elif msgSize == 6:
        if (result < pow(2, 14)) or (result >= pow(2, 21)):
            return False
    elif msgSize == 7:
        if (result < pow(2, 21)) or (result >= pow(2, 28)):
            return False
    elif msgSize == 8:
        if result < pow(2, 28):
            return False

    return True
