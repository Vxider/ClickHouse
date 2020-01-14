#pragma once

#include <Core/SettingsCommon.h>


namespace DB
{
class ASTStorage;

/** Settings for the Socket engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct SocketSettings : public SettingsCollection<SocketSettings>
{
#define LIST_OF_SOCKET_SETTINGS(M) \
    M(SettingString, socket_host, "", "socket hostname.") \
    M(SettingUInt64, socket_port, 0, "socket port.") \
    M(SettingString, socket_format, "", "The message format for Socket engine.") \
    M(SettingChar, socket_row_delimiter, '\0', "The character to be considered as a delimiter in Socket message.") \
    M(SettingUInt64, socket_max_block_size, 0, "The maximum block size per table for Socket engine.") \
    M(SettingUInt64, socket_skip_broken_messages, 0, "Skip at least this number of broken messages from socket topic per block") \
    M(SettingUInt64, socket_zmq_parallelism, 0, "zmq parallelism") \
    M(SettingUInt64, socket_timeout, 5000, "socket timeout in milliseconds")

    DECLARE_SETTINGS_COLLECTION(LIST_OF_SOCKET_SETTINGS)

    void loadFromQuery(ASTStorage & storage_def);
};

}
