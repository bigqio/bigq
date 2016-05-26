using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using Newtonsoft.Json;

namespace BigQ
{
    public class BigQHelper
    {
        public static bool TCPMessageWrite(TcpClient Client, BigQMessage Message)
        {
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (Client == null)
                {
                    Log("*** TCPMessageWrite null client supplied");
                    return false;
                }

                if (Message == null)
                {
                    Log("** TCPMessageWrite null message supplied");
                    return false;
                }

                #endregion

                #region Add-Values-if-Needed

                if (Message.CreatedUTC == null) Message.CreatedUTC = DateTime.Now.ToUniversalTime();
                if (String.IsNullOrEmpty(Message.MessageId)) Message.MessageId = Guid.NewGuid().ToString();

                if (Message.Data == null || Message.Data.Length < 1) Message.ContentLength = 0;
                else Message.ContentLength = Message.Data.Length;

                #endregion

                #region Process

                SourceIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;
                Byte[] MessageAsBytes = Message.ToBytes();
                Client.GetStream().Write(MessageAsBytes, 0, MessageAsBytes.Length);
                Client.GetStream().Flush();

                //
                // enumerate
                //
                // if (Message.Data != null) Log("TCPMessageWrite " + SourceIp + ":" + SourcePort + " sent " + Message.Data.Length + " content bytes in message of " + MessageAsBytes.Length + " bytes");
                // else Log("TCPMessageWrite " + SourceIp + ":" + SourcePort + " sent (null) content bytes");
                // Log(Message.ToString());
                //
                return true;

                #endregion
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** TCPMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return false;
            }
            catch (SocketException SockInner)
            {
                Log("*** TCPMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return false;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** TCPMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return false;
            }
            catch (IOException IOInner)
            {
                Log("*** TCPMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return false;
            }
            catch (Exception EInner)
            {
                Log("*** TCPMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("TCPMessageWrite " + SourceIp + ":" + SourcePort, EInner);
                return false;
            }
        }

        public static bool TCPMessageRead(TcpClient Client, out BigQMessage Message)
        {
            Message = null;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (Client == null)
                {
                    Log("*** TCPMessageRead null client supplied");
                    return false;
                }

                #endregion

                #region Variables

                int BytesRead = 0;
                int sleepInterval = 25;
                int maxTimeout = 500;
                int currentTimeout = 0;
                bool timeout = false;

                SourceIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;
                NetworkStream ClientStream = Client.GetStream();

                byte[] headerBytes;
                byte[] contentBytes;
                
                #endregion

                #region Read-Headers-from-Stream

                if (!IsTCPPeerConnected(Client))
                {
                    Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected while attempting to read headers");
                    return false;
                }
                
                if (!ClientStream.CanRead)
                {
                    Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " stream marked as unreadble while attempting to read headers");
                    return false;
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

                    do
                    {
                        int read = ClientStream.Read(headerBuffer, 0, headerBuffer.Length);
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
                            Log("TCPMessageRead Last four bytes: " + 
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
                                // Log("TCPMessageRead reached end of headers after " + BytesRead + " bytes");
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
                                    Thread.Sleep(sleepInterval);
                                }
                            }

                            if (timeout) break;
                        }
                    }
                    while (ClientStream.DataAvailable);

                    if (timeout)
                    {
                        Log("*** TCPMessageRead timeout " + currentTimeout + "ms/" + maxTimeout + "ms exceeded while reading headers after reading " + BytesRead + " bytes");
                        return false;
                    }

                    headerBytes = headerMs.ToArray();

                    #endregion

                    #region Process-Headers-into-Message

                    try
                    {
                        Message = new BigQMessage(headerBytes);
                    }
                    catch (Exception e)
                    {
                        Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " while reading message headers: " + e.Message);
                        return false;
                    }

                    if (Message == null || Message == default(BigQMessage))
                    {
                        Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " null or default message after reading headers");
                        return false;
                    }

                    #endregion

                    #region Check-for-Empty-ContentLength

                    if (Message.ContentLength == null) return true;
                    if (Message.ContentLength <= 0) return true;

                    #endregion
                }
                
                #endregion

                #region Read-Data-from-Stream

                using (MemoryStream dataMs = new MemoryStream())
                {
                    long bytesRemaining = Convert.ToInt64(Message.ContentLength);
                    timeout = false;
                    currentTimeout = 0;

                    do
                    {
                        byte[] buffer;
                        long bufferSize = 2048;
                        
                        //
                        // reduce buffer size if number of bytes remaining is
                        // less than the pre-defined buffer size of 2KB
                        //
                        if (bytesRemaining < bufferSize) bufferSize = bytesRemaining;
                        buffer = new byte[bufferSize];

                        int read = ClientStream.Read(buffer, 0, buffer.Length);
                        if (read > 0)
                        {
                            dataMs.Write(buffer, 0, read);
                            BytesRead = BytesRead + read;
                            bytesRemaining = bytesRemaining - read;
                        }

                        //
                        // check if read fully
                        //
                        if (bytesRemaining == 0) break;
                        if (BytesRead == Message.ContentLength) break;

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
                                    Thread.Sleep(sleepInterval);
                                }
                            }

                            if (timeout) break;
                        }
                    }
                    while (ClientStream.DataAvailable);

                    if (timeout)
                    {
                        Log("*** TCPMessageRead timeout " + currentTimeout + "ms/" + maxTimeout + "ms exceeded while reading content after reading " + BytesRead + " bytes");
                        return false;
                    }

                    contentBytes = dataMs.ToArray();
                }

                #endregion

                #region Check-Content-Bytes-and-Assign

                if (contentBytes == null || contentBytes.Length < 1)
                {
                    Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " no content read");
                    return false;
                }

                if (contentBytes.Length != Message.ContentLength)
                {
                    Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " content length " + contentBytes.Length + " bytes does not match header value of " + Message.ContentLength);
                    return false;
                }

                Message.Data = new byte[contentBytes.Length];
                Buffer.BlockCopy(contentBytes, 0, Message.Data, 0, contentBytes.Length);

                #endregion
                
                //
                // enumerate
                //
                // if (Message.Data != null) Log("TCPMessageRead " + SourceIp + ":" + SourcePort + " returning " + Message.Data.Length + " content bytes");
                // else Log("TCPMessageRead " + SourceIp + ":" + SourcePort + " returning (null) content bytes");
                // Log(Message.ToString());
                return true;
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return false;
            }
            catch (SocketException SockInner)
            {
                Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return false;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return false;
            }
            catch (IOException IOInner)
            {
                Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return false;
            }
            catch (Exception EInner)
            {
                Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("TCPMessageRead " + SourceIp + ":" + SourcePort, EInner);
                return false;
            }
        }

        public static bool TCPSocketWrite(TcpClient Client, byte[] Data)
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

                if (Client == null)
                {
                    Log("*** TCPSocketWrite null client supplied");
                    return false;
                }

                if (Data == null || Data.Length < 1)
                {
                    Log("*** TCPSocketWrite null data supplied");
                    return false;
                }

                #endregion

                #region Process

                SourceIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;

                Client.GetStream().Write(Data, 0, Data.Length);
                Client.GetStream().Flush();
                return true;

                #endregion
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** TCPSocketWrite " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return false;
            }
            catch (SocketException SockInner)
            {
                Log("*** TCPSocketWrite " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return false;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** TCPSocketWrite " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return false;
            }
            catch (IOException IOInner)
            {
                Log("*** TCPSocketWrite " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return false;
            }
            catch (Exception EInner)
            {
                Log("*** TCPSocketWrite " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("TCPSocketWrite " + SourceIp + ":" + SourcePort, EInner);
                return false;
            }
        }

        public static bool TCPSocketRead(TcpClient Client, out byte[] Data)
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

                if (Client == null)
                {
                    Log("*** TCPSocketRead null client supplied");
                    return false;
                }

                #endregion

                #region Variables

                int BytesRead = 0;
                int sleepInterval = 1;
                int maxSleep = 100;
                int currentSleep = 0;
                SourceIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;
                NetworkStream ClientStream = Client.GetStream();

                #endregion

                #region Read-from-Stream

                if (!IsTCPPeerConnected(Client))
                {
                    Log("*** TCPSocketRead " + SourceIp + ":" + SourcePort + " disconnected");
                    return false;
                }

                //
                //
                // original
                //
                //
                /*
                byte[] buffer = new byte[2048];
                using (MemoryStream ms = new MemoryStream())
                {
                    int read;

                    while ((read = ClientStream.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        // Log("Read " + read + " bytes from stream from peer " + SourceIp + ":" + SourcePort);
                        ms.Write(buffer, 0, read);
                        BytesRead += read;

                        // if (read < buffer.Length) break;
                        if (!ClientStream.DataAvailable) break;
                    }

                    Data = ms.ToArray();
                }

                if (Data == null || Data.Length < 1)
                {
                    // Log("No data read from " + SourceIp + ":" + SourcePort);
                    return false;
                }
                */

                //
                //
                // new
                //
                //
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
                                        Thread.Sleep(sleepInterval);
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
                    Log("*** TCPSocketRead stream marked as unreadble");
                    return false;
                }

                #endregion

                if (Data == null || Data.Length < 1)
                {
                    // Log("TCPSocketRead no data read from " + SourceIp + ":" + SourcePort);
                    return false;
                }

                Log("TCPSocketRead returning " + Data.Length + " bytes from " + SourceIp + ":" + SourcePort);
                return true;
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** TCPSocketRead " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return false;
            }
            catch (SocketException SockInner)
            {
                Log("*** TCPSocketRead " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return false;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** TCPSocketRead " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return false;
            }
            catch (IOException IOInner)
            {
                Log("*** TCPSocketRead " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return false;
            }
            catch (Exception EInner)
            {
                Log("*** TCPSocketRead " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("TCPSocketRead " + SourceIp + ":" + SourcePort, EInner);
                return false;
            }
        }

        public static async Task<bool> WSMessageWrite(HttpListenerContext Context, WebSocket Client, BigQMessage Message)
        {
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (Context == null)
                {
                    Log("*** WSMessageWrite null context supplied");
                    return false;
                }

                if (Client == null)
                {
                    Log("*** WSMessageWrite null client supplied");
                    return false;
                }

                if (Message == null)
                {
                    Log("** WSMessageWrite null message supplied");
                    return false;
                }

                #endregion

                #region Add-Values-if-Needed

                if (Message.CreatedUTC == null) Message.CreatedUTC = DateTime.Now.ToUniversalTime();
                if (String.IsNullOrEmpty(Message.MessageId)) Message.MessageId = Guid.NewGuid().ToString();

                if (Message.Data == null || Message.Data.Length < 1) Message.ContentLength = 0;
                else Message.ContentLength = Message.Data.Length;

                #endregion

                #region Process
                
                SourceIp = Context.Request.RemoteEndPoint.Address.ToString();
                SourcePort = Context.Request.RemoteEndPoint.Port;
                Byte[] MessageAsBytes = Message.ToBytes();
                await Client.SendAsync(new ArraySegment<byte>(MessageAsBytes, 0, MessageAsBytes.Length), WebSocketMessageType.Binary, true, CancellationToken.None);

                //
                // enumerate
                //
                // if (Message.Data != null) Log("WSMessageWrite " + SourceIp + ":" + SourcePort + " sent " + Message.Data.Length + " content bytes in message of " + MessageAsBytes.Length + " bytes");
                // else Log("WSMessageWrite " + SourceIp + ":" + SourcePort + " sent (null) content bytes");
                // Log(Message.ToString());
                //
                return true;

                #endregion
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** WSMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return false;
            }
            catch (SocketException SockInner)
            {
                Log("*** WSMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return false;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** WSMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return false;
            }
            catch (IOException IOInner)
            {
                Log("*** WSMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return false;
            }
            catch (Exception EInner)
            {
                Log("*** WSMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("WSMessageWrite " + SourceIp + ":" + SourcePort, EInner);
                return false;
            }
        }

        public static async Task<BigQMessage> WSMessageRead(HttpListenerContext Context, WebSocket Client)
        {
            BigQMessage Message = null;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (Context == null)
                {
                    Log("*** WSMessageRead null context supplied");
                    return null;
                }

                if (Client == null)
                {
                    Log("*** WSMessageRead null client supplied");
                    return null;
                }

                #endregion

                #region Variables

                int BytesRead = 0;
                SourceIp = Context.Request.RemoteEndPoint.Address.ToString();
                SourcePort = Context.Request.RemoteEndPoint.Port;

                byte[] headerBytes;
                byte[] contentBytes;

                #endregion

                #region Read-Headers-from-Stream

                if (!IsWSPeerConnected(Client))
                {
                    Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " disconnected while attempting to read headers");
                    return null;
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
                    
                    while (Client.State == WebSocketState.Open)
                    {
                        WebSocketReceiveResult receiveResult = await Client.ReceiveAsync(new ArraySegment<byte>(headerBuffer), CancellationToken.None);
                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            await Client.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
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
                            /*
                            Log("TCPMessageRead Last four bytes: " + 
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
                                // Log("TCPMessageRead reached end of headers after " + BytesRead + " bytes");
                                break;
                            }
                        }   
                    }
                    
                    headerBytes = headerMs.ToArray();

                    #endregion

                    #region Process-Headers-into-Message

                    try
                    {
                        Message = new BigQMessage(headerBytes);
                    }
                    catch (Exception e)
                    {
                        Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " while reading message headers: " + e.Message);
                        return null;
                    }

                    if (Message == null || Message == default(BigQMessage))
                    {
                        Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " null or default message after reading headers");
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
                    byte[] buffer;
                    long bufferSize = 2048;

                    while (Client.State == WebSocketState.Open)
                    {
                        //
                        // reduce buffer size if number of bytes remaining is
                        // less than the pre-defined buffer size of 2KB
                        //
                        if (bytesRemaining < bufferSize) bufferSize = bytesRemaining;
                        buffer = new byte[bufferSize];

                        WebSocketReceiveResult receiveResult = await Client.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
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

                if (contentBytes == null || contentBytes.Length < 1)
                {
                    Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " no content read");
                    return null;
                }

                if (contentBytes.Length != Message.ContentLength)
                {
                    Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " content length " + contentBytes.Length + " bytes does not match header value of " + Message.ContentLength);
                    return null;
                }

                Message.Data = new byte[contentBytes.Length];
                Buffer.BlockCopy(contentBytes, 0, Message.Data, 0, contentBytes.Length);

                #endregion

                //
                // enumerate
                //
                // if (Message.Data != null) Log("TCPMessageRead " + SourceIp + ":" + SourcePort + " returning " + Message.Data.Length + " content bytes");
                // else Log("TCPMessageRead " + SourceIp + ":" + SourcePort + " returning (null) content bytes");
                // Log(Message.ToString());
                return Message;
            }
            catch (WebSocketException WSEInner)
            {
                Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " disconnected (websocket exception): " + WSEInner.Message);
                return null;
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return null;
            }
            catch (SocketException SockInner)
            {
                Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return null;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return null;
            }
            catch (IOException IOInner)
            {
                Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return null;
            }
            catch (Exception EInner)
            {
                Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("WSMessageRead " + SourceIp + ":" + SourcePort, EInner);
                return null;
            }
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

        public static bool IsTCPPeerConnected(TcpClient Client)
        {
            // see http://stackoverflow.com/questions/6993295/how-to-determine-if-the-tcp-is-connected-or-not

            bool success = false;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (Client == null)
                {
                    Log("*** IsTCPPeerConnected null client supplied");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                success = false;
                SourceIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;

                if (Client != null
                    && Client.Client != null
                    && Client.Client.Connected)
                {
                    if (Client.Client.Poll(0, SelectMode.SelectRead))
                    {
                        byte[] buff = new byte[1];
                        if (Client.Client.Receive(buff, SocketFlags.Peek) == 0) success = false;
                        else success = true;
                    }

                    success = true;
                }
                else
                {
                    success = false;
                }

                return success;

                #endregion
            }
            catch
            {
                return false;
            }
        }

        public static bool IsWSPeerConnected(WebSocket Client)
        {
            //
            //
            // Unsure if there are other means of doing this
            //
            //

            bool success = false;

            try
            {
                #region Check-for-Null-Values

                if (Client == null)
                {
                    Log("*** IsWSPeerConnected null client supplied");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                if (Client.State == WebSocketState.Open) success = true;
                else success = false;
                return success;

                #endregion
            }
            catch
            {
                return false;
            }
        }

        public static T DeserializeJson<T>(string json, bool debug)
        {
            if (debug)
            {
                Log("");
                Log("DeserializeJson input:");
                Log(json);
                Log("");
            }

            Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings();
            settings.NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore;
            return (T)Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json, settings);
        }

        public static T DeserializeJson<T>(byte[] bytes, bool debug)
        {
            if (debug)
            {
                Log("");
                Log("DeserializeJson input:");
                Log(Encoding.UTF8.GetString(bytes));
                Log("");
            }

            Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings();
            string json = Encoding.UTF8.GetString(bytes);
            return (T)Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json, settings);
        }

        public static T JObjectToObject<T>(object obj)
        {
            Newtonsoft.Json.Linq.JObject jobject = (Newtonsoft.Json.Linq.JObject)obj;
            T ret = jobject.ToObject<T>();
            return ret;
        }

        public static T JArrayToList<T>(object obj)
        {
            // 
            // Call with List<T> as the type, i.e.
            // foo = JArrayToList<List<foo>>(data);
            //

            if (obj == null) throw new ArgumentNullException("obj");
            Newtonsoft.Json.Linq.JArray jarray = (Newtonsoft.Json.Linq.JArray)obj;
            return jarray.ToObject<T>();
        }

        public static string SerializeJson(object obj)
        {
            string json = JsonConvert.SerializeObject(obj, Newtonsoft.Json.Formatting.Indented, new JsonSerializerSettings { });
            return json;
        }

        public static T CopyObject<T>(T source)
        {
            string json = SerializeJson(source);
            T ret = DeserializeJson<T>(json, false);
            return ret;
        }

        public static void Log(string message)
        {
            Console.WriteLine(message);
        }

        public static void LogException(string method, Exception e)
        {
            Log("================================================================================");
            Log(" = Method: " + method);
            Log(" = Exception Type: " + e.GetType().ToString());
            Log(" = Exception Data: " + e.Data);
            Log(" = Inner Exception: " + e.InnerException);
            Log(" = Exception Message: " + e.Message);
            Log(" = Exception Source: " + e.Source);
            Log(" = Exception StackTrace: " + e.StackTrace);
            Log("================================================================================");
        }

        private static bool SanitizeString(string dirty, out string clean)
        {
            clean = null;

            try
            {
                if (String.IsNullOrEmpty(dirty)) return true;

                // null, below ASCII range, above ASCII range
                for (int i = 0; i < dirty.Length; i++)
                {
                    if (((int)(dirty[i]) == 0) ||    // null
                        ((int)(dirty[i]) < 32) ||    // below ASCII range
                        ((int)(dirty[i]) > 126)      // above ASCII range
                        )
                    {
                        continue;
                    }
                    else
                    {
                        clean += dirty[i];
                    }
                }

                return true;
            }
            catch (Exception EInner)
            {
                LogException("SanitizeString", EInner);
                return false;
            }
        }

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
    }
}
