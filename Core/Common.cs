using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigQ.Core
{
    /// <summary>
    /// A series of helpful methods for BigQ including messaging, framing, sockets and websockets, sanitization, serialization, and more.
    /// </summary>
    public static class Common
    {
        #region TCP-Methods

        public static bool TCPMessageWrite(TcpClient CurrentClient, Message CurrentMessage)
        {
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null) return false;
                if (CurrentMessage == null) return false;

                #endregion

                #region Add-Values-if-Needed

                if (CurrentMessage.CreatedUtc == null) CurrentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
                if (String.IsNullOrEmpty(CurrentMessage.MessageID)) CurrentMessage.MessageID = Guid.NewGuid().ToString();

                if (CurrentMessage.Data == null || CurrentMessage.Data.Length < 1) CurrentMessage.ContentLength = 0;
                else CurrentMessage.ContentLength = CurrentMessage.Data.Length;

                #endregion

                #region Process

                SourceIp = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Port;
                Byte[] MessageAsBytes = CurrentMessage.ToBytes();
                CurrentClient.GetStream().Write(MessageAsBytes, 0, MessageAsBytes.Length);
                CurrentClient.GetStream().Flush();

                //
                // enumerate
                //
                // if (Message.Data != null) Logging.Log(LoggingModule.Severity.Debug, "TCPMessageWrite " + SourceIp + ":" + SourcePort + " sent " + Message.Data.Length + " content bytes in message of " + MessageAsBytes.Length + " bytes");
                // else Logging.Log(LoggingModule.Severity.Debug, "TCPMessageWrite " + SourceIp + ":" + SourcePort + " sent (null) content bytes");
                // Log(Message.ToString());
                //
                return true;

                #endregion
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static bool TCPMessageRead(TcpClient CurrentClient, out Message CurrentMessage)
        {
            CurrentMessage = null;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null) return false;
                if (!CurrentClient.Connected) return false;

                #endregion

                #region Variables

                int BytesRead = 0;
                int sleepInterval = 25;
                int maxTimeout = 500;
                int currentTimeout = 0;
                bool timeout = false;

                SourceIp = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Port;
                NetworkStream ClientStream = null;

                try
                {
                    ClientStream = CurrentClient.GetStream();
                }
                catch (Exception)
                {
                    return false;
                }

                byte[] headerBytes;
                byte[] contentBytes;

                #endregion

                #region Read-Headers-from-Stream

                if (!ClientStream.CanRead)
                {
                    return false;
                }

                if (!ClientStream.DataAvailable)
                {
                    return true;
                }

                using (MemoryStream headerMs = new MemoryStream())
                {
                    #region Read-Header-Bytes

                    byte[] headerBuffer = new byte[1];
                    byte[] lastFourBytes = new byte[4];
                    lastFourBytes[0] = 0x00; // least recent
                    lastFourBytes[1] = 0x00;
                    lastFourBytes[2] = 0x00;
                    lastFourBytes[3] = 0x00; // most recent

                    timeout = false;
                    currentTimeout = 0;
                    int read = 0;

                    while ((read = ClientStream.ReadAsync(headerBuffer, 0, headerBuffer.Length).Result) > 0)
                    {
                        if (read > 0)
                        {
                            headerMs.Write(headerBuffer, 0, read);
                            BytesRead += read;

                            //
                            // reset timeout since there was a successful read
                            //
                            currentTimeout = 0;
                        }

                        if (read == 1)
                        {
                            //
                            // shift last four bytes
                            //
                            lastFourBytes[0] = lastFourBytes[1];
                            lastFourBytes[1] = lastFourBytes[2];
                            lastFourBytes[2] = lastFourBytes[3];
                            lastFourBytes[3] = headerBuffer[0];
                        }

                        if (BytesRead > 3)
                        {
                            //
                            // check if end of headers reached
                            //
                            /*
                            Logging.Log(LoggingModule.Severity.Debug, "TCPMessageRead Last four bytes: " + 
                                (int)lastFourBytes[3] + " " + (int)lastFourBytes[2] + " " + (int)lastFourBytes[1] + " " + (int)lastFourBytes[0] + 
                                "   " +
                                (char)lastFourBytes[3] + " " + (char)lastFourBytes[2] + " " + (char)lastFourBytes[1] + " " + (char)lastFourBytes[0]
                                );
                            */
                            if ((int)lastFourBytes[0] == 13
                                && (int)lastFourBytes[1] == 10
                                && (int)lastFourBytes[2] == 13
                                && (int)lastFourBytes[3] == 10)
                            {
                                // Logging.Log(LoggingModule.Severity.Debug, "TCPMessageRead reached end of headers after " + BytesRead + " bytes");
                                break;
                            }
                        }

                        if (!ClientStream.DataAvailable)
                        {
                            while (true)
                            {
                                if (currentTimeout >= maxTimeout)
                                {
                                    timeout = true;
                                    break;
                                }
                                else
                                {
                                    currentTimeout += sleepInterval;
                                    Task.Delay(sleepInterval).Wait();
                                }
                            }

                            if (timeout) break;
                        }
                    }

                    if (timeout)
                    {
                        return false;
                    }

                    headerBytes = headerMs.ToArray();
                    if (headerBytes == null || headerBytes.Length < 1)
                    {
                        return true;
                    }

                    #endregion

                    #region Process-Headers-into-Message

                    try
                    {
                        CurrentMessage = new Message(headerBytes);
                    }
                    catch (Exception)
                    {
                        return false;
                    }

                    if (CurrentMessage == null || CurrentMessage == default(Message))
                    {
                        return false;
                    }

                    #endregion

                    #region Check-for-Empty-ContentLength

                    if (CurrentMessage.ContentLength == null) return true;
                    if (CurrentMessage.ContentLength <= 0) return true;

                    #endregion
                }

                #endregion

                #region Read-Data-from-Stream

                using (MemoryStream dataMs = new MemoryStream())
                {
                    long bytesRemaining = Convert.ToInt64(CurrentMessage.ContentLength);
                    timeout = false;
                    currentTimeout = 0;

                    int read = 0;
                    byte[] buffer;
                    long bufferSize = 2048;
                    if (bufferSize > bytesRemaining) bufferSize = bytesRemaining;
                    buffer = new byte[bufferSize];

                    while ((read = ClientStream.ReadAsync(buffer, 0, buffer.Length).Result) > 0)
                    {
                        if (read > 0)
                        {
                            dataMs.Write(buffer, 0, read);
                            BytesRead = BytesRead + read;
                            bytesRemaining = bytesRemaining - read;
                        }

                        //
                        // reduce buffer size if number of bytes remaining is
                        // less than the pre-defined buffer size of 2KB
                        //
                        // Console.WriteLine("Bytes remaining " + bytesRemaining + ", buffer size " + bufferSize);
                        if (bytesRemaining < bufferSize)
                        {
                            bufferSize = bytesRemaining;
                            // Console.WriteLine("Adjusting buffer size to " + bytesRemaining);
                        }

                        buffer = new byte[bufferSize];

                        //
                        // check if read fully
                        //
                        if (bytesRemaining == 0) break;
                        if (BytesRead == CurrentMessage.ContentLength) break;

                        if (!ClientStream.DataAvailable)
                        {
                            while (true)
                            {
                                if (currentTimeout >= maxTimeout)
                                {
                                    timeout = true;
                                    break;
                                }
                                else
                                {
                                    currentTimeout += sleepInterval;
                                    Task.Delay(sleepInterval).Wait();
                                }
                            }

                            if (timeout) break;
                        }
                    }

                    if (timeout)
                    {
                        return false;
                    }

                    contentBytes = dataMs.ToArray();
                }

                #endregion

                #region Check-Content-Bytes-and-Assign

                if (contentBytes == null || contentBytes.Length < 1)
                {
                    return true;
                }

                if (contentBytes.Length != CurrentMessage.ContentLength)
                {
                    return true;
                }

                CurrentMessage.Data = new byte[contentBytes.Length];
                Buffer.BlockCopy(contentBytes, 0, CurrentMessage.Data, 0, contentBytes.Length);

                #endregion

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static bool TCPSocketWrite(TcpClient CurrentClient, byte[] Data)
        {
            //
            //
            // This method has been deprecated, do not use
            // It does not contain any message framing
            //
            // Use TCPMessageWrite instead
            //
            //
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null) return false;
                if (Data == null || Data.Length < 1) return false;

                #endregion

                #region Process

                SourceIp = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Port;

                CurrentClient.GetStream().Write(Data, 0, Data.Length);
                CurrentClient.GetStream().Flush();
                return true;

                #endregion
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static bool TCPSocketRead(TcpClient CurrentClient, out byte[] Data)
        {
            //
            //
            // This method has been deprecated, do not use
            // It does not contain any message framing
            //
            // Use TCPMessageRead instead
            //
            //
            Data = null;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null) return false;

                #endregion

                #region Variables

                int BytesRead = 0;
                int sleepInterval = 1;
                int maxSleep = 100;
                int currentSleep = 0;
                SourceIp = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Port;
                NetworkStream ClientStream = CurrentClient.GetStream();

                #endregion

                #region Read-from-Stream

                if (ClientStream.CanRead)
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        byte[] buffer = new byte[262144];

                        do
                        {
                            int read = ClientStream.Read(buffer, 0, buffer.Length);
                            if (read > 0)
                            {
                                ms.Write(buffer, 0, read);
                                BytesRead += read;
                            }

                            if (!ClientStream.DataAvailable)
                            {
                                while (true)
                                {
                                    if (currentSleep >= maxSleep)
                                    {
                                        break;
                                    }
                                    else
                                    {
                                        currentSleep += sleepInterval;
                                        Task.Delay(sleepInterval).Wait();
                                    }
                                }
                            }
                        }
                        while (ClientStream.DataAvailable);

                        Data = ms.ToArray();
                    }
                }
                else
                {
                    return false;
                }

                #endregion

                if (Data == null || Data.Length < 1)
                {
                    return false;
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        #endregion

        #region TCP-SSL-Methods

        public static bool TCPSSLMessageWrite(TcpClient CurrentClient, SslStream CurrentSSLStream, Message CurrentMessage)
        {
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null) return false;
                if (CurrentSSLStream == null) return false;
                if (CurrentMessage == null) return false;

                #endregion

                #region Add-Values-if-Needed

                if (CurrentMessage.CreatedUtc == null) CurrentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
                if (String.IsNullOrEmpty(CurrentMessage.MessageID)) CurrentMessage.MessageID = Guid.NewGuid().ToString();

                if (CurrentMessage.Data == null || CurrentMessage.Data.Length < 1) CurrentMessage.ContentLength = 0;
                else CurrentMessage.ContentLength = CurrentMessage.Data.Length;

                #endregion

                #region Process

                SourceIp = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Port;
                Byte[] MessageAsBytes = CurrentMessage.ToBytes();
                CurrentSSLStream.Write(MessageAsBytes, 0, MessageAsBytes.Length);
                CurrentSSLStream.Flush();

                return true;

                #endregion
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static Message TCPSSLMessageRead(TcpClient CurrentClient, SslStream CurrentSSLStream)
        {
            Message Message = new Message();
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null) return null;
                if (!CurrentClient.Connected) return null;

                #endregion

                #region Variables

                int BytesRead = 0;
                SourceIp = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Port;
                byte[] headerBytes;
                byte[] contentBytes;

                #endregion

                #region Read-Headers-from-Stream

                if (!CurrentSSLStream.CanRead) return null;

                using (MemoryStream headerMs = new MemoryStream())
                {
                    #region Read-Header-Bytes

                    byte[] headerBuffer = new byte[1];
                    byte[] lastFourBytes = new byte[4];
                    lastFourBytes[0] = 0x00; // least recent
                    lastFourBytes[1] = 0x00;
                    lastFourBytes[2] = 0x00;
                    lastFourBytes[3] = 0x00; // most recent
                    int read = 0;

                    while ((read = CurrentSSLStream.ReadAsync(headerBuffer, 0, headerBuffer.Length).Result) > 0)
                    {
                        if (read > 0)
                        {
                            headerMs.Write(headerBuffer, 0, read);
                            BytesRead += read;
                        }

                        if (read == 1)
                        {
                            //
                            // shift last four bytes
                            //
                            lastFourBytes[0] = lastFourBytes[1];
                            lastFourBytes[1] = lastFourBytes[2];
                            lastFourBytes[2] = lastFourBytes[3];
                            lastFourBytes[3] = headerBuffer[0];
                        }

                        if (BytesRead > 3)
                        {
                            //
                            // check if end of headers reached
                            //
                            if ((int)lastFourBytes[0] == 13
                                && (int)lastFourBytes[1] == 10
                                && (int)lastFourBytes[2] == 13
                                && (int)lastFourBytes[3] == 10)
                            {
                                break;
                            }
                        }
                    }

                    headerBytes = headerMs.ToArray();
                    if (headerBytes == null || headerBytes.Length < 1) return null;

                    #endregion

                    #region Process-Headers-into-Message

                    try
                    {
                        Message = new Message(headerBytes);
                    }
                    catch (Exception)
                    {
                        return null;
                    }

                    if (Message == null || Message == default(Message))
                    {
                        return null;
                    }

                    #endregion

                    #region Check-for-Empty-ContentLength

                    if (Message.ContentLength == null) return Message;
                    if (Message.ContentLength <= 0) return Message;

                    #endregion
                }

                #endregion

                #region Read-Data-from-Stream

                using (MemoryStream dataMs = new MemoryStream())
                {
                    long bytesRemaining = Convert.ToInt64(Message.ContentLength);
                    int read = 0;
                    byte[] buffer;
                    long bufferSize = 2048;
                    if (bufferSize > bytesRemaining) bufferSize = bytesRemaining;
                    buffer = new byte[bufferSize];

                    while ((read = CurrentSSLStream.ReadAsync(buffer, 0, buffer.Length).Result) > 0)
                    {
                        if (read > 0)
                        {
                            dataMs.Write(buffer, 0, read);
                            BytesRead = BytesRead + read;
                            bytesRemaining = bytesRemaining - read;
                        }

                        //
                        // reduce buffer size if number of bytes remaining is
                        // less than the pre-defined buffer size of 2KB
                        //
                        if (bytesRemaining < bufferSize) bufferSize = bytesRemaining;
                        buffer = new byte[bufferSize];

                        //
                        // check if read fully
                        //
                        if (bytesRemaining == 0) break;
                        if (BytesRead == Message.ContentLength) break;
                    }

                    contentBytes = dataMs.ToArray();
                }

                #endregion

                #region Check-Content-Bytes-and-Assign

                if (contentBytes == null || contentBytes.Length < 1) return null;
                if (contentBytes.Length != Message.ContentLength) return null;

                Message.Data = new byte[contentBytes.Length];
                Buffer.BlockCopy(contentBytes, 0, Message.Data, 0, contentBytes.Length);

                #endregion

                return Message;
            }
            catch (Exception)
            {
                return null;
            }
        }

        #endregion

        #region Websocket-Methods

        public static async Task<bool> WSMessageWrite(HttpListenerContext CurrentContext, WebSocket CurrentWSClient, Message CurentMessage)
        {
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentContext == null) return false;
                if (CurrentWSClient == null) return false;
                if (CurentMessage == null) return false;

                #endregion

                #region Add-Values-if-Needed

                if (CurentMessage.CreatedUtc == null) CurentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
                if (String.IsNullOrEmpty(CurentMessage.MessageID)) CurentMessage.MessageID = Guid.NewGuid().ToString();

                if (CurentMessage.Data == null || CurentMessage.Data.Length < 1) CurentMessage.ContentLength = 0;
                else CurentMessage.ContentLength = CurentMessage.Data.Length;

                #endregion

                #region Process

                SourceIp = CurrentContext.Request.RemoteEndPoint.Address.ToString();
                SourcePort = CurrentContext.Request.RemoteEndPoint.Port;
                Byte[] MessageAsBytes = CurentMessage.ToBytes();
                await CurrentWSClient.SendAsync(new ArraySegment<byte>(MessageAsBytes, 0, MessageAsBytes.Length), WebSocketMessageType.Binary, true, CancellationToken.None);

                return true;

                #endregion
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static async Task<Message> WSMessageRead(HttpListenerContext CurrentContext, WebSocket CurrentWSClient)
        {
            Message Message = null;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentContext == null) return null;
                if (CurrentWSClient == null) return null;

                #endregion

                #region Variables

                int BytesRead = 0;
                SourceIp = CurrentContext.Request.RemoteEndPoint.Address.ToString();
                SourcePort = CurrentContext.Request.RemoteEndPoint.Port;

                byte[] headerBytes;
                byte[] contentBytes;

                #endregion

                #region Read-Headers-from-Stream

                using (MemoryStream headerMs = new MemoryStream())
                {
                    #region Read-Header-Bytes

                    byte[] headerBuffer = new byte[1];
                    byte[] lastFourBytes = new byte[4];
                    lastFourBytes[0] = 0x00; // least recent
                    lastFourBytes[1] = 0x00;
                    lastFourBytes[2] = 0x00;
                    lastFourBytes[3] = 0x00; // most recent

                    while (CurrentWSClient.State == WebSocketState.Open)
                    {
                        WebSocketReceiveResult receiveResult = await CurrentWSClient.ReceiveAsync(new ArraySegment<byte>(headerBuffer), CancellationToken.None);
                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            await CurrentWSClient.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
                        }

                        headerMs.Write(headerBuffer, 0, 1);
                        BytesRead++;

                        //
                        // shift last four bytes
                        //
                        lastFourBytes[0] = lastFourBytes[1];
                        lastFourBytes[1] = lastFourBytes[2];
                        lastFourBytes[2] = lastFourBytes[3];
                        lastFourBytes[3] = headerBuffer[0];

                        if (BytesRead > 3)
                        {
                            //
                            // check if end of headers reached
                            // 
                            if ((int)lastFourBytes[0] == 13
                                && (int)lastFourBytes[1] == 10
                                && (int)lastFourBytes[2] == 13
                                && (int)lastFourBytes[3] == 10)
                            {
                                break;
                            }
                        }
                    }

                    headerBytes = headerMs.ToArray();
                    if (headerBytes == null || headerBytes.Length < 1) return null;

                    #endregion

                    #region Process-Headers-into-Message

                    try
                    {
                        Message = new Message(headerBytes);
                    }
                    catch (Exception)
                    {
                        return null;
                    }

                    if (Message == null || Message == default(Message)) return null;

                    #endregion

                    #region Check-for-Empty-ContentLength

                    if (Message.ContentLength == null) return Message;
                    if (Message.ContentLength <= 0) return Message;

                    #endregion
                }

                #endregion

                #region Read-Data-from-Stream

                using (MemoryStream dataMs = new MemoryStream())
                {
                    long bytesRemaining = Convert.ToInt64(Message.ContentLength);
                    byte[] buffer;
                    long bufferSize = 2048;

                    while (CurrentWSClient.State == WebSocketState.Open)
                    {
                        //
                        // reduce buffer size if number of bytes remaining is
                        // less than the pre-defined buffer size of 2KB
                        //
                        if (bytesRemaining < bufferSize) bufferSize = bytesRemaining;
                        buffer = new byte[bufferSize];

                        WebSocketReceiveResult receiveResult = await CurrentWSClient.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            //
                            // end of message
                            //
                        }
                        else
                        {
                            dataMs.Write(buffer, 0, buffer.Length);
                            BytesRead = BytesRead + buffer.Length;
                            bytesRemaining = bytesRemaining - buffer.Length;

                            //
                            // check if read fully
                            //
                            if (bytesRemaining == 0) break;
                            if (BytesRead == Message.ContentLength) break;
                        }
                    }

                    contentBytes = dataMs.ToArray();
                }

                #endregion

                #region Check-Content-Bytes-and-Assign

                if (contentBytes == null || contentBytes.Length < 1) return null;
                if (contentBytes.Length != Message.ContentLength) return null;

                Message.Data = new byte[contentBytes.Length];
                Buffer.BlockCopy(contentBytes, 0, Message.Data, 0, contentBytes.Length);

                #endregion

                return Message;
            }
            catch (Exception)
            {
                return null;
            }
        }

        #endregion

        #region Websocket-SSL-Methods

        public static async Task<bool> WSSSLMessageWrite(HttpListenerContext CurrentContext, WebSocket CurrentWSClient, Message CurentMessage)
        {
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentContext == null) return false;
                if (CurrentWSClient == null) return false;
                if (CurentMessage == null) return false;

                #endregion

                #region Add-Values-if-Needed

                if (CurentMessage.CreatedUtc == null) CurentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
                if (String.IsNullOrEmpty(CurentMessage.MessageID)) CurentMessage.MessageID = Guid.NewGuid().ToString();

                if (CurentMessage.Data == null || CurentMessage.Data.Length < 1) CurentMessage.ContentLength = 0;
                else CurentMessage.ContentLength = CurentMessage.Data.Length;

                #endregion

                #region Process

                SourceIp = CurrentContext.Request.RemoteEndPoint.Address.ToString();
                SourcePort = CurrentContext.Request.RemoteEndPoint.Port;
                Byte[] MessageAsBytes = CurentMessage.ToBytes();
                await CurrentWSClient.SendAsync(new ArraySegment<byte>(MessageAsBytes, 0, MessageAsBytes.Length), WebSocketMessageType.Binary, true, CancellationToken.None);

                return true;

                #endregion
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static async Task<Message> WSSSLMessageRead(HttpListenerContext CurrentContext, WebSocket CurrentWSClient)
        {
            Message Message = null;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentContext == null) return null;
                if (CurrentWSClient == null) return null;

                #endregion

                #region Variables

                int BytesRead = 0;
                SourceIp = CurrentContext.Request.RemoteEndPoint.Address.ToString();
                SourcePort = CurrentContext.Request.RemoteEndPoint.Port;

                byte[] headerBytes;
                byte[] contentBytes;

                #endregion

                #region Read-Headers-from-Stream

                using (MemoryStream headerMs = new MemoryStream())
                {
                    #region Read-Header-Bytes

                    byte[] headerBuffer = new byte[1];
                    byte[] lastFourBytes = new byte[4];
                    lastFourBytes[0] = 0x00; // least recent
                    lastFourBytes[1] = 0x00;
                    lastFourBytes[2] = 0x00;
                    lastFourBytes[3] = 0x00; // most recent

                    while (CurrentWSClient.State == WebSocketState.Open)
                    {
                        WebSocketReceiveResult receiveResult = await CurrentWSClient.ReceiveAsync(new ArraySegment<byte>(headerBuffer), CancellationToken.None);
                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            await CurrentWSClient.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
                        }

                        headerMs.Write(headerBuffer, 0, 1);
                        BytesRead++;

                        //
                        // shift last four bytes
                        //
                        lastFourBytes[0] = lastFourBytes[1];
                        lastFourBytes[1] = lastFourBytes[2];
                        lastFourBytes[2] = lastFourBytes[3];
                        lastFourBytes[3] = headerBuffer[0];

                        if (BytesRead > 3)
                        {
                            //
                            // check if end of headers reached
                            // 
                            if ((int)lastFourBytes[0] == 13
                                && (int)lastFourBytes[1] == 10
                                && (int)lastFourBytes[2] == 13
                                && (int)lastFourBytes[3] == 10)
                            {
                                break;
                            }
                        }
                    }

                    headerBytes = headerMs.ToArray();
                    if (headerBytes == null || headerBytes.Length < 1) return null;

                    #endregion

                    #region Process-Headers-into-Message

                    try
                    {
                        Message = new Message(headerBytes);
                    }
                    catch (Exception)
                    {
                        return null;
                    }

                    if (Message == null || Message == default(Message)) return null;

                    #endregion

                    #region Check-for-Empty-ContentLength

                    if (Message.ContentLength == null) return Message;
                    if (Message.ContentLength <= 0) return Message;

                    #endregion
                }

                #endregion

                #region Read-Data-from-Stream

                using (MemoryStream dataMs = new MemoryStream())
                {
                    long bytesRemaining = Convert.ToInt64(Message.ContentLength);
                    byte[] buffer;
                    long bufferSize = 2048;

                    while (CurrentWSClient.State == WebSocketState.Open)
                    {
                        //
                        // reduce buffer size if number of bytes remaining is
                        // less than the pre-defined buffer size of 2KB
                        //
                        if (bytesRemaining < bufferSize) bufferSize = bytesRemaining;
                        buffer = new byte[bufferSize];

                        WebSocketReceiveResult receiveResult = await CurrentWSClient.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            //
                            // end of message
                            //
                        }
                        else
                        {
                            dataMs.Write(buffer, 0, buffer.Length);
                            BytesRead = BytesRead + buffer.Length;
                            bytesRemaining = bytesRemaining - buffer.Length;

                            //
                            // check if read fully
                            //
                            if (bytesRemaining == 0) break;
                            if (BytesRead == Message.ContentLength) break;
                        }
                    }

                    contentBytes = dataMs.ToArray();
                }

                #endregion

                #region Check-Content-Bytes-and-Assign

                if (contentBytes == null || contentBytes.Length < 1) return null;
                if (contentBytes.Length != Message.ContentLength) return null;

                Message.Data = new byte[contentBytes.Length];
                Buffer.BlockCopy(contentBytes, 0, Message.Data, 0, contentBytes.Length);

                #endregion

                return Message;
            }
            catch (Exception)
            {
                return null;
            }
        }

        #endregion

        #region Conversion-and-Serialization

        public static string BytesToHex(byte[] ba)
        {
            if (ba == null || ba.Length < 1) return null;
            string hex = BitConverter.ToString(ba);
            return hex.Replace("-", "");
        }

        public static byte[] HexToBytes(string hex)
        {
            if (String.IsNullOrEmpty(hex)) return null;
            int numChars = hex.Length;
            byte[] bytes = new byte[numChars / 2];
            for (int i = 0; i < numChars; i += 2)
                bytes[i / 2] = Convert.ToByte(hex.Substring(i, 2), 16);
            return bytes;
        }

        public static object BytesToObject(byte[] Data)
        {
            using (var ms = new MemoryStream())
            {
                var bf = new BinaryFormatter();
                ms.Write(Data, 0, Data.Length);
                ms.Seek(0, SeekOrigin.Begin);
                var obj = bf.Deserialize(ms);
                return obj;
            }
        }

        public static byte[] ObjectToBytes(object obj)
        {
            if (obj == null) return null;
            if (obj is byte[]) return (byte[])obj;

            BinaryFormatter bf = new BinaryFormatter();
            using (var ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
        }

        public static T DeserializeJson<T>(string json)
        {
            // Newtonsoft
            JsonSerializerSettings settings = new JsonSerializerSettings();
            settings.NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore;
            return (T)Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json, settings);

            // System.Web.Script.Serialization
            // JavaScriptSerializer ser = new JavaScriptSerializer();
            // ser.MaxJsonLength = Int32.MaxValue;
            // return (T)ser.Deserialize<T>(json);
        }

        public static T DeserializeJson<T>(byte[] bytes)
        {
            // Newtonsoft
            JsonSerializerSettings settings = new JsonSerializerSettings();
            string json = Encoding.UTF8.GetString(bytes);
            return (T)Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json, settings);

            // System.Web.Script.Serialization
            // JavaScriptSerializer ser = new JavaScriptSerializer();
            // ser.MaxJsonLength = Int32.MaxValue;
            // return (T)ser.Deserialize<T>(Encoding.UTF8.GetString(bytes));
        }

        public static string SerializeJson(object obj)
        {
            // Newtonsoft
            JsonSerializerSettings settings = new JsonSerializerSettings();
            settings.NullValueHandling = NullValueHandling.Ignore;
            string json = JsonConvert.SerializeObject(obj, Newtonsoft.Json.Formatting.Indented, settings);
            return json;

            // System.Web.Script.Serialization
            // JavaScriptSerializer ser = new JavaScriptSerializer();
            // ser.MaxJsonLength = Int32.MaxValue;
            // string json = ser.Serialize(obj);
            // return json;
        }

        public static T CopyObject<T>(T source)
        {
            string json = SerializeJson(source);
            T ret = DeserializeJson<T>(json);
            return ret;
        }

        public static List<object> ObjectToList(object o)
        {
            if (o == null) return null;
            List<object> ret = new List<object>();
            var enumerator = ((IEnumerable)o).GetEnumerator();
            while (enumerator.MoveNext())
            {
                ret.Add(enumerator.Current);
            }
            return ret;
        }

        public static string StringListToCsv(List<string> vals)
        {
            try
            {
                if (vals == null) return null;
                if (vals.Count < 1) return null;
                string ret = "";
                int count = 0;

                foreach (string curr in vals)
                {
                    if (count == 0)
                    {
                        ret += curr;
                    }
                    else
                    {
                        ret += "," + curr;
                    }

                    count++;
                }

                return ret;
            }
            catch (Exception)
            {
                return null;
            }
        }

        public static List<int> CsvToIntList(string csv)
        {
            try
            {
                if (String.IsNullOrEmpty(csv)) return null;

                List<int> ret = new List<int>();

                string[] array = csv.Split(',');

                if (array != null)
                {
                    if (array.Length > 0)
                    {
                        foreach (string curr in array)
                        {
                            if (String.IsNullOrEmpty(curr)) continue;

                            int val = 0;
                            if (!Int32.TryParse(curr, out val))
                            {
                                return null;
                            }

                            ret.Add(val);
                        }

                        return ret;
                    }
                }

                return null;
            }
            catch (Exception)
            {
                return null;
            }
        }

        public static List<string> CsvToStringList(string csv)
        {
            try
            {
                if (String.IsNullOrEmpty(csv)) return null;

                List<string> ret = new List<string>();

                string[] array = csv.Split(',');

                if (array != null)
                {
                    if (array.Length > 0)
                    {
                        foreach (string curr in array)
                        {
                            if (String.IsNullOrEmpty(curr)) continue;
                            ret.Add(curr.Trim());
                        }

                        return ret;
                    }
                }

                return null;
            }
            catch (Exception)
            {
                return null;
            }
        }

        public static List<T> GenericToSpecificList<T>(List<object> source)
        {
            if (source == null) return null;

            List<T> retList = new List<T>();
            int count = 0;

            foreach (object curr in source)
            {
                retList.Add((T)curr);
                count++;
            }

            return retList;
        }

        public static bool DataTableIsNullOrEmpty(DataTable table)
        {
            if (table == null) return true;
            if (table.Rows.Count < 1) return true;
            return false;
        }

        public static List<object> DataTableToListObject(DataTable dt, string objType)
        {
            //
            // Must pass in the fully-qualified class name including namespace
            //
            // i.e. Namespace.ClassName
            //

            if (dt == null) return null;
            if (dt.Rows.Count < 1) return null;
            if (String.IsNullOrEmpty(objType)) return null;

            List<object> retList = new List<object>();
            int count = 0;

            foreach (DataRow currRow in dt.Rows)
            {
                object ret = Activator.CreateInstance(Type.GetType(objType));
                if (ret == null) return null;

                foreach (PropertyInfo prop in ret.GetType().GetProperties())
                {
                    #region Process-Each-Property

                    PropertyInfo tempProp = prop;

                    switch (prop.PropertyType.ToString().ToLower().Trim())
                    {
                        case "system.string":
                            string valStr = currRow[prop.Name].ToString().Trim();
                            tempProp.SetValue(ret, valStr, null);
                            break;

                        case "system.int32":
                        case "system.nullable`1[system.int32]":
                            int valInt32 = 0;
                            if (Int32.TryParse(currRow[prop.Name].ToString(), out valInt32)) tempProp.SetValue(ret, valInt32, null);
                            break;

                        case "system.int64":
                        case "system.nullable`1[system.int64]":
                            long valInt64 = 0;
                            if (Int64.TryParse(currRow[prop.Name].ToString(), out valInt64)) tempProp.SetValue(ret, valInt64, null);
                            break;

                        case "system.decimal":
                        case "system.nullable`1[system.decimal]":
                            decimal valDecimal = 0m;
                            if (Decimal.TryParse(currRow[prop.Name].ToString(), out valDecimal)) tempProp.SetValue(ret, valDecimal, null);
                            break;

                        case "system.datetime":
                        case "system.nullable`1[system.datetime]":
                            DateTime datetime = DateTime.Now;
                            if (DateTime.TryParse(currRow[prop.Name].ToString(), out datetime)) tempProp.SetValue(ret, datetime, null);
                            break;

                        default:
                            break;
                    }

                    #endregion
                }

                count++;
                retList.Add(ret);
            }

            return retList;
        }

        public static object DataTableToObject(DataTable dt, string objType)
        {
            if (dt == null) return null;
            if (dt.Rows.Count != 1) return null;
            if (String.IsNullOrEmpty(objType)) return null;

            object ret = new object();

            foreach (DataRow dr in dt.Rows)
            {
                ret = Activator.CreateInstance(Type.GetType(objType));
                if (ret == null)
                {
                    return null;
                }

                foreach (PropertyInfo prop in ret.GetType().GetProperties())
                {
                    PropertyInfo tempProp = prop;

                    switch (prop.PropertyType.ToString().ToLower().Trim())
                    {
                        case "system.string":
                            string valStr = dr[prop.Name].ToString().Trim();
                            tempProp.SetValue(ret, valStr, null);
                            break;

                        case "system.int32":
                        case "system.nullable`1[system.int32]":
                            int valInt32 = 0;
                            if (Int32.TryParse(dr[prop.Name].ToString(), out valInt32)) tempProp.SetValue(ret, valInt32, null);
                            break;

                        case "system.int64":
                        case "system.nullable`1[system.int64]":
                            long valInt64 = 0;
                            if (Int64.TryParse(dr[prop.Name].ToString(), out valInt64)) tempProp.SetValue(ret, valInt64, null);
                            break;

                        case "system.decimal":
                        case "system.nullable`1[system.decimal]":
                            decimal valDecimal = 0m;
                            if (Decimal.TryParse(dr[prop.Name].ToString(), out valDecimal)) tempProp.SetValue(ret, valDecimal, null);
                            break;

                        case "system.datetime":
                        case "system.nullable`1[system.datetime]":
                            DateTime datetime = DateTime.Now;
                            if (DateTime.TryParse(dr[prop.Name].ToString(), out datetime)) tempProp.SetValue(ret, datetime, null);
                            break;

                        default:
                            break;
                    }
                }

                break;
            }

            return ret;
        }

        public static T DataTableToObject<T>(DataTable table) where T : new()
        {
            IList<PropertyInfo> properties = typeof(T).GetProperties().ToList();
            IList<T> result = new List<T>();

            foreach (var row in table.Rows)
            {
                var item = CreateItemFromRow<T>((DataRow)row, properties);
                return item;
            }

            return default(T);
        }

        public static T DataRowToObject<T>(DataRow row) where T : new()
        {
            IList<PropertyInfo> properties = typeof(T).GetProperties().ToList();
            IList<T> result = new List<T>();

            var item = CreateItemFromRow<T>((DataRow)row, properties);
            return item;
        }

        public static IList<T> DataTableToList<T>(this DataTable table) where T : new()
        {
            IList<PropertyInfo> properties = typeof(T).GetProperties().ToList();
            IList<T> result = new List<T>();

            foreach (var row in table.Rows)
            {
                var item = CreateItemFromRow<T>((DataRow)row, properties);
                result.Add(item);
            }

            return result;
        }

        public static IList<T> DataTableToList<T>(this DataTable table, Dictionary<string, string> mappings) where T : new()
        {
            IList<PropertyInfo> properties = typeof(T).GetProperties().ToList();
            IList<T> result = new List<T>();

            foreach (var row in table.Rows)
            {
                var item = CreateItemFromRow<T>((DataRow)row, properties, mappings);
                result.Add(item);
            }

            return result;
        }

        private static T CreateItemFromRow<T>(DataRow row, IList<PropertyInfo> properties) where T : new()
        {
            T item = new T();
            foreach (var property in properties)
            {
                if (row[property.Name] is System.DBNull) continue;
                property.SetValue(item, row[property.Name], null);
            }
            return item;
        }

        private static T CreateItemFromRow<T>(DataRow row, IList<PropertyInfo> properties, Dictionary<string, string> mappings) where T : new()
        {
            T item = new T();
            foreach (var property in properties)
            {
                if (mappings.ContainsKey(property.Name))
                    property.SetValue(item, row[mappings[property.Name]], null);
            }
            return item;
        }

        public static List<dynamic> DataTableToListDynamic(DataTable dt)
        {
            List<dynamic> ret = new List<dynamic>();
            if (dt == null || dt.Rows.Count < 1) return ret;

            foreach (DataRow curr in dt.Rows)
            {
                dynamic dyn = new ExpandoObject();
                foreach (DataColumn col in dt.Columns)
                {
                    var dic = (IDictionary<string, object>)dyn;
                    dic[col.ColumnName] = curr[col];
                }
                ret.Add(dyn);
            }

            return ret;
        }

        public static dynamic DataTableToDynamic(DataTable dt)
        {
            dynamic ret = new ExpandoObject();
            if (dt == null || dt.Rows.Count < 1) return ret;

            foreach (DataRow curr in dt.Rows)
            {
                foreach (DataColumn col in dt.Columns)
                {
                    var dic = (IDictionary<string, object>)ret;
                    dic[col.ColumnName] = curr[col];
                }

                return ret;
            }

            return ret;
        }

        public static List<Dictionary<string, object>> DataTableToListDictionary(DataTable dt)
        {
            List<Dictionary<string, object>> ret = new List<Dictionary<string, object>>();
            if (dt == null || dt.Rows.Count < 1) return ret;

            foreach (DataRow curr in dt.Rows)
            {
                Dictionary<string, object> currDict = new Dictionary<string, object>();

                foreach (DataColumn col in dt.Columns)
                {
                    currDict.Add(col.ColumnName, curr[col]);
                }

                ret.Add(currDict);
            }

            return ret;
        }

        public static Dictionary<string, object> DataTableToDictionary(DataTable dt)
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            if (dt == null || dt.Rows.Count < 1) return ret;

            foreach (DataRow curr in dt.Rows)
            {
                foreach (DataColumn col in dt.Columns)
                {
                    ret.Add(col.ColumnName, curr[col]);
                }

                return ret;
            }

            return ret;
        }

        public static Dictionary<string, object> ObjectToDictionary(object obj)
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();

            foreach (PropertyInfo prop in obj.GetType().GetProperties())
            {
                string propName = prop.Name;
                var val = obj.GetType().GetProperty(propName).GetValue(obj, null);
                if (val != null)
                {
                    ret.Add(propName, val.ToString());
                }
            }

            return ret;
        }

        public static bool IsDictionary(object obj)
        {
            if (obj == null) return false;
            return obj is IDictionary &&
                   obj.GetType().IsGenericType &&
                   obj.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(Dictionary<,>));
        }

        public static T DictionaryToObject<T>(Dictionary<string, object> dict) where T : new()
        {
            string json = SerializeJson(dict);
            return DeserializeJson<T>(json);
        }

        public static string DictionaryToString(Dictionary<string, object> dict)
        {
            if (dict == null) return null;
            string ret = "";
            ret += Environment.NewLine;
            foreach (KeyValuePair<string, object> curr in dict)
            {
                if (String.IsNullOrEmpty(curr.Key)) continue;
                if (curr.Value == null) ret += "  " + curr.Key + ": (null)" + Environment.NewLine;
                else ret += "  " + curr.Key + ": " + curr.Value.ToString() + Environment.NewLine;
            }
            ret += Environment.NewLine;
            return ret;
        }

        #endregion

        #region Input

        public static bool InputBoolean(string question, bool yesDefault)
        {
            Console.Write(question);

            if (yesDefault) Console.Write(" [Y/n]? ");
            else Console.Write(" [y/N]? ");

            string userInput = Console.ReadLine();

            if (String.IsNullOrEmpty(userInput))
            {
                if (yesDefault) return true;
                return false;
            }

            userInput = userInput.ToLower();

            if (yesDefault)
            {
                if (
                    (String.Compare(userInput, "n") == 0)
                    || (String.Compare(userInput, "no") == 0)
                   )
                {
                    return false;
                }

                return true;
            }
            else
            {
                if (
                    (String.Compare(userInput, "y") == 0)
                    || (String.Compare(userInput, "yes") == 0)
                   )
                {
                    return true;
                }

                return false;
            }
        }

        public static List<string> InputStringList(string question)
        {
            Console.WriteLine("Press ENTER with no data to end");
            List<string> ret = new List<string>();
            while (true)
            {
                Console.Write(question + " ");
                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput)) break;
                ret.Add(userInput);
            }
            return ret;
        }

        public static string InputString(string question, string defaultAnswer, bool allowNull)
        {
            while (true)
            {
                Console.Write(question);

                if (!String.IsNullOrEmpty(defaultAnswer))
                {
                    Console.Write(" [" + defaultAnswer + "]");
                }

                Console.Write(" ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    if (!String.IsNullOrEmpty(defaultAnswer)) return defaultAnswer;
                    if (allowNull) return null;
                    else continue;
                }

                return userInput;
            }
        }

        public static int InputInteger(string question, int defaultAnswer, bool positiveOnly, bool allowZero)
        {
            while (true)
            {
                Console.Write(question);
                Console.Write(" [" + defaultAnswer + "] ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    return defaultAnswer;
                }

                int ret;
                if (!Int32.TryParse(userInput, out ret))
                {
                    Console.WriteLine("Please enter a valid integer.");
                    continue;
                }

                if (ret == 0 && allowZero)
                {
                    return 0;
                }

                if (ret < 0 && positiveOnly)
                {
                    Console.WriteLine("Please enter a value greater than zero.");
                    continue;
                }

                return ret;
            }
        }

        public static decimal InputDecimal(string question, decimal defaultAnswer, bool positiveOnly, bool allowZero)
        {
            while (true)
            {
                Console.Write(question);
                Console.Write(" [" + defaultAnswer + "] ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    return defaultAnswer;
                }

                decimal ret;
                if (!Decimal.TryParse(userInput, out ret))
                {
                    Console.WriteLine("Please enter a valid decimal.");
                    continue;
                }

                if (ret == 0 && allowZero)
                {
                    return 0;
                }

                if (ret < 0 && positiveOnly)
                {
                    Console.WriteLine("Please enter a value greater than zero.");
                    continue;
                }

                return ret;
            }
        }

        #endregion

        #region Is-True

        public static bool IsTrue(int? val)
        {
            if (val == null) return false;
            if (Convert.ToInt32(val) == 1) return true;
            return false;
        }

        public static bool IsTrue(int val)
        {
            if (val == 1) return true;
            return false;
        }

        public static bool IsTrue(bool val)
        {
            return val;
        }

        public static bool IsTrue(bool? val)
        {
            if (val == null) return false;
            return Convert.ToBoolean(val);
        }

        public static bool IsTrue(string val)
        {
            if (String.IsNullOrEmpty(val)) return false;
            val = val.ToLower().Trim();
            int val_int = 0;
            if (Int32.TryParse(val, out val_int)) if (val_int == 1) return true;
            if (String.Compare(val, "true") == 0) return true;
            return false;
        }

        public static bool IsList(object o)
        {
            if (o == null) return false;
            return o is IList &&
                   o.GetType().IsGenericType &&
                   o.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>));
        }

        #endregion

        #region Misc

        public static string StackToString()
        {
            string ret = "";

            StackTrace t = new StackTrace();
            for (int i = 0; i < t.FrameCount; i++)
            {
                if (i == 0)
                {
                    ret += t.GetFrame(i).GetMethod().Name;
                }
                else
                {
                    ret += " <= " + t.GetFrame(i).GetMethod().Name;
                }
            }

            return ret;
        }

        #endregion
    }
}
