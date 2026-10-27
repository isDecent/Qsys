using System;
using System.Linq;
using Crestron.SimplSharp;
using Crestron.SimplSharp.CrestronSockets;
using QscQsys.ModuleFramework.Events;
using QscQsys.ModuleFramework.Logging;

namespace QscQsys.Communications.Sockets
{
    /// <summary>
    /// Represents a TCP client for socket communication.
    /// </summary>
    public sealed class TcpClient : AbstractSocketCommunicator
    {
        private const int RetryDelay = 2500;
        private const int ConnectTimeout = 10000;

        private TCPClient _client;
        private readonly CTimer _retryTimer;
        private readonly CTimer _receiveTimeoutTimer;
        private bool _disconnectRequested;
        private int _receiveTimeout;

        /// <summary>
        /// Gets or sets the time in milliseconds without received data after which the connection
        /// is considered lost and is re-established. 0 disables the check.
        /// </summary>
        public int ReceiveTimeout
        {
            get { lock (MainLock) return _receiveTimeout; }
            set
            {
                lock (MainLock)
                {
                    _receiveTimeout = value;

                    if (ProtectedConnected)
                        ResetReceiveTimeout();
                    else
                        _receiveTimeoutTimer.Stop();
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the TcpClient class.
        /// </summary>
        public TcpClient()
        {
            _retryTimer = new CTimer(x =>
                Reconnect(), Timeout.Infinite);
            _receiveTimeoutTimer = new CTimer(x =>
                ReceiveTimedOut(), Timeout.Infinite);
        }

        /// <summary>
        /// Initializes a new instance of the TcpClient class with the specified ID.
        /// </summary>
        /// <param name="id">The ID of the client.</param>
        public TcpClient(string id)
            : base(id)
        {
            _retryTimer = new CTimer(x =>
                Reconnect(), Timeout.Infinite);
            _receiveTimeoutTimer = new CTimer(x =>
                ReceiveTimedOut(), Timeout.Infinite);
        }

        /// <summary>
        /// Initializes a new instance of the TcpClient class with the specified logger.
        /// </summary>
        /// <param name="logger">The logger to use for logging.</param>
        public TcpClient(ILogger logger)
            : base(logger)
        {
            _retryTimer = new CTimer(x =>
                Reconnect(), Timeout.Infinite);
            _receiveTimeoutTimer = new CTimer(x =>
                ReceiveTimedOut(), Timeout.Infinite);
        }

        /// <summary>
        /// Initializes a new instance of the TcpClient class with the specified ID and logger.
        /// </summary>
        /// <param name="id">The ID of the client.</param>
        /// <param name="logger">The logger to use for logging.</param>
        public TcpClient(string id, ILogger logger)
            : base(id, logger)
        {
            _retryTimer = new CTimer(x =>
                Reconnect(), Timeout.Infinite);
            _receiveTimeoutTimer = new CTimer(x =>
                ReceiveTimedOut(), Timeout.Infinite);
        }

        private void ConnectToServerCallback(TCPClient client)
        {
            lock (MainLock)
            {
                // Ignore callbacks from a client that has already been replaced
                if (ProtectedDisposed || client != _client)
                    return;

                if (client.ClientStatus == SocketStatus.SOCKET_STATUS_CONNECTED)
                {
                    if (!ProtectedConnected)
                        Connected(client);
                    return;
                }
                WaitAndTryReconnect();
            }
        }

        /// <summary>
        /// Must be called while holding MainLock.
        /// </summary>
        private void Connected(TCPClient client)
        {
            _retryTimer.Stop();
            ProtectedConnected = true;
            ResetReceiveTimeout();
            client.ReceiveDataAsync(ReceiveCallback);

            RaiseConnectionEvents(SocketStatus.SOCKET_STATUS_CONNECTED);
        }

        private void ReceiveCallback(TCPClient client, int numBytes)
        {
            lock (MainLock)
            {
                if (ProtectedDisposed || client == null || client != _client)
                    return;

                // 0 or less bytes means the socket was closed or broken
                if (numBytes <= 0)
                {
                    Logger.LogWarning("Connection closed by remote host");
                    ConnectionLost(SocketStatus.SOCKET_STATUS_BROKEN_REMOTELY);
                    return;
                }

                ResetReceiveTimeout();

                var response = new string(client.IncomingDataBuffer.Take(numBytes).Select(b => (char)b).ToArray());
                OnResponseReceived(new StringEventArgs(response));
            }

            var error = client.ReceiveDataAsync(ReceiveCallback);
            if (error == SocketErrorCodes.SOCKET_OK || error == SocketErrorCodes.SOCKET_OPERATION_PENDING)
                return;

            lock (MainLock)
            {
                if (ProtectedDisposed || client != _client)
                    return;

                Logger.LogWarning("ReceiveDataAsync error: {0}", GetSocketErrorCodeName(error));
                ConnectionLost(SocketStatus.SOCKET_STATUS_BROKEN_LOCALLY);
            }
        }

        private void Client_SocketStatusChange(TCPClient client, SocketStatus clientSocketStatus)
        {
            lock (MainLock)
            {
                if (ProtectedDisposed || client != _client)
                    return;

                var status = GetSocketStatusName(clientSocketStatus);
                Logger.LogNotice("SocketStatus changed {0}", status);

                if (clientSocketStatus != SocketStatus.SOCKET_STATUS_CONNECTED)
                {
                    ConnectionLost(clientSocketStatus);
                    return;
                }

                // Already handled by ConnectToServerCallback; arming a second receive would fail
                if (ProtectedConnected)
                    return;

                Connected(client);
            }
        }

        private void RaiseConnectionEvents(SocketStatus clientSocketStatus)
        {
            ConnectionStatus = (ushort)clientSocketStatus;

            OnConnectedChange(new BoolEventArgs(ProtectedConnected));
            OnConnectionStatusChange(
                new ConnectionStatusChangeEventArgs(new ConnectionStatusChangePayload((ushort)clientSocketStatus,
                    GetSocketStatusName(clientSocketStatus))));
        }

        /// <summary>
        /// Marks the connection as lost, reports it, and schedules a reconnect if a connection is still wanted.
        /// Must be called while holding MainLock.
        /// </summary>
        private void ConnectionLost(SocketStatus clientSocketStatus)
        {
            _receiveTimeoutTimer.Stop();
            ProtectedConnected = false;

            RaiseConnectionEvents(clientSocketStatus);

            WaitAndTryReconnect();
        }

        private void ResetReceiveTimeout()
        {
            if (_receiveTimeout > 0)
                _receiveTimeoutTimer.Reset(_receiveTimeout);
        }

        private void ReceiveTimedOut()
        {
            lock (MainLock)
            {
                if (ProtectedDisposed || !ProtectedConnected || _receiveTimeout <= 0)
                    return;

                Logger.LogWarning("No data received for {0} ms, connection is assumed lost", _receiveTimeout);
                ConnectionLost(SocketStatus.SOCKET_STATUS_LINK_LOST);
            }
        }

        /// <summary>
        /// Connects to the TCP server.
        /// </summary>
        public override void Connect()
        {
            var ipaddress = IpAddress;
            var port = Port;
            Connect(ipaddress, port);
        }

        /// <summary>
        /// Connects to the TCP server at the specified IP address and port.
        /// </summary>
        /// <param name="ipAddress">The IP address of the TCP server.</param>
        /// <param name="port">The port number of the TCP server.</param>
        public override void Connect(string ipAddress, ushort port)
        {
            ThrowIfDisposed();

            TCPClient oldClient;

            lock (MainLock)
            {
                ProtectedIpAddress = ipAddress;
                ProtectedPort = port;

                if (string.IsNullOrEmpty(ipAddress) || port <= 0)
                {
                    Logger.LogError("Host is null or emptry and/or port is set to 0");
                    return;
                }

                Logger.PrintLine("Connection starting {0}:{1}...", ipAddress, port);
                Logger.LogNotice("Connection starting {0}:{1}...", ipAddress, port);

                ConnectionRequested = true;
                _disconnectRequested = false;
                oldClient = StartNewClient();
            }

            DisposeClient(oldClient);
        }

        private void Reconnect()
        {
            TCPClient oldClient;

            lock (MainLock)
            {
                if (ProtectedDisposed)
                    return;

                if (ProtectedConnected || _disconnectRequested || !ConnectionRequested)
                    return;

                Logger.PrintLine("Connection was not established, retrying...");
                oldClient = StartNewClient();
            }

            DisposeClient(oldClient);
        }

        /// <summary>
        /// Replaces the current client with a new one and starts connecting it. A fresh client is used for
        /// every attempt because a TCPClient can get stuck after a failed or broken connection.
        /// Must be called while holding MainLock. Returns the previous client, which the caller must dispose
        /// after releasing MainLock.
        /// </summary>
        private TCPClient StartNewClient()
        {
            _retryTimer.Stop();
            _receiveTimeoutTimer.Stop();

            var oldClient = DetachClient();

            if (ProtectedConnected)
            {
                ProtectedConnected = false;
                RaiseConnectionEvents(SocketStatus.SOCKET_STATUS_NO_CONNECT);
            }

            _client = new TCPClient(ProtectedIpAddress, ProtectedPort, 65535);
            _client.SocketStatusChange += Client_SocketStatusChange;

            var error = _client.ConnectToServerAsync(ConnectToServerCallback);
            if (error != SocketErrorCodes.SOCKET_OK)
            {
                Logger.PrintLine("ConnectToServerAsync error: {0}", GetSocketErrorCodeName(error));
                WaitAndTryReconnect();
            }
            else
            {
                // Retry if the connect callback never arrives
                _retryTimer.Reset(ConnectTimeout);
            }

            return oldClient;
        }

        /// <summary>
        /// Must be called while holding MainLock.
        /// </summary>
        private TCPClient DetachClient()
        {
            var client = _client;
            _client = null;

            if (client != null)
                client.SocketStatusChange -= Client_SocketStatusChange;

            return client;
        }

        private void DisposeClient(TCPClient client)
        {
            if (client == null)
                return;

            try
            {
                client.DisconnectFromServer();
                client.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogException(ex);
            }
        }

        /// <summary>
        /// Must be called while holding MainLock.
        /// </summary>
        private void WaitAndTryReconnect()
        {
            if (ProtectedDisposed || _disconnectRequested || !ConnectionRequested)
                return;

            _retryTimer.Reset(RetryDelay);
        }

        /// <summary>
        /// Disconnects from the TCP server.
        /// </summary>
        public override void Disconnect()
        {
            ThrowIfDisposed();

            TCPClient oldClient;

            lock (MainLock)
            {
                if (ProtectedDisposed)
                    return;

                ConnectionRequested = false;
                _disconnectRequested = true;
                _retryTimer.Stop();
                _receiveTimeoutTimer.Stop();

                if (_client == null)
                    return;

                Logger.LogNotice("Connection closing...");
                Logger.PrintLine("Connection closing...");

                oldClient = DetachClient();

                if (ProtectedConnected)
                {
                    ProtectedConnected = false;
                    RaiseConnectionEvents(SocketStatus.SOCKET_STATUS_NO_CONNECT);
                }
            }

            DisposeClient(oldClient);
        }

        /// <summary>
        /// Sends a command to the TCP server.
        /// </summary>
        /// <param name="command">The command to send.</param>
        public override void SendCommand(string command)
        {
            var data = command.ToCharArray().Select(c => (byte)c).ToArray();
            SendCommand(data);
        }

        /// <summary>
        /// Sends a command to the TCP server.
        /// </summary>
        /// <param name="command">The command to send as a character array.</param>
        public override void SendCommand(char[] command)
        {
            if (command == null)
                return;

            var data = new byte[command.Length];

            for (var i = 0; i < command.Length; i++)
            {
                data[i] = Convert.ToByte(command[i]);
            }

            SendCommand(data);
        }

        /// <summary>
        /// Sends a command to the TCP server.
        /// </summary>
        /// <param name="command">The command to send as a byte array.</param>
        public override void SendCommand(byte[] command)
        {
            ThrowIfDisposed();

            if (command == null)
                return;

            try
            {
                TCPClient client;
                lock (MainLock)
                {
                    if (!ProtectedConnected || _client == null) return;
                    client = _client;
                }

                var c = new string(command.Take(command.Length).Select(b => (char)b).ToArray());
                Logger.PrintLine("Sending command -->{0}<--", c.ReplaceHex());
                var error = client.SendData(command, command.Length);

                if (error == SocketErrorCodes.SOCKET_OK)
                    return;

                lock (MainLock)
                {
                    if (ProtectedDisposed || client != _client || !ProtectedConnected)
                        return;

                    Logger.LogWarning("SendData error: {0}", GetSocketErrorCodeName(error));
                    ConnectionLost(SocketStatus.SOCKET_STATUS_BROKEN_LOCALLY);
                }
            }
            catch (Exception ex)
            {
                Logger.LogException(ex);
            }
        }

        public override void Dispose()
        {
            ThrowIfDisposed();

            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (ProtectedDisposed)
                return;

            TCPClient oldClient = null;

            lock (MainLock)
            {
                if (ProtectedDisposed)
                    return;

                ProtectedDisposed = true;

                if (disposing)
                {
                    _retryTimer.Stop();
                    _retryTimer.Dispose();
                    _receiveTimeoutTimer.Stop();
                    _receiveTimeoutTimer.Dispose();

                    oldClient = DetachClient();

                    ProtectedConnected = false;
                }
            }

            DisposeClient(oldClient);
        }
    }
}