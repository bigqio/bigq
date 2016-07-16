using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Script.Serialization;

namespace BigQ
{
    /// <summary>
    /// A series of helpful methods for BigQ including messaging, framing, sockets and websockets, sanitization, serialization, and more.
    /// </summary>
    public class Helper
    {
        #region TCP-Methods

        public static bool TCPMessageWrite(TcpClient CurrentClient, Message CurrentMessage, bool ConsoleLogResponseTime)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** TCPMessageWrite null client supplied");
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("** TCPMessageWrite null message supplied");
                    return false;
                }

                #endregion

                #region Add-Values-if-Needed

                if (CurrentMessage.CreatedUTC == null) CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
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
            finally
            {
                sw.Stop();
                if (ConsoleLogResponseTime) Log("TCPMessageWrite " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        public static Message TCPMessageRead(TcpClient CurrentClient, bool ConsoleLogResponseTime)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Message Message = new Message();
            bool DataAvailable = false;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** TCPMessageRead null client supplied");
                    return null;
                }

                if (!CurrentClient.Connected)
                {
                    Log("*** TCPMessageRead supplied client is not connected");
                    return null;
                }

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
                catch (Exception e)
                {
                    Log("*** TCPMessageRead disconnected while attaching to stream for " + SourceIp + ":" + SourcePort + ": " + e.Message);
                    return null;
                }

                byte[] headerBytes;
                byte[] contentBytes;
                
                #endregion

                #region Read-Headers-from-Stream

                if (!IsTCPPeerConnected(CurrentClient))
                {
                    Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected while attempting to read headers");
                    return null;
                }
                
                if (!ClientStream.CanRead)
                {
                    // Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " stream marked as unreadble while attempting to read headers");
                    // Thread.Sleep(25);
                    return null;
                }

                if (!ClientStream.DataAvailable)
                {
                    // Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " stream has no data available");
                    // Thread.Sleep(25);
                    return null;
                }

                DataAvailable = true;

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
                    
                    if (timeout)
                    {
                        Log("*** TCPMessageRead timeout " + currentTimeout + "ms/" + maxTimeout + "ms exceeded while reading headers after reading " + BytesRead + " bytes");
                        return null;
                    }

                    headerBytes = headerMs.ToArray();
                    if (headerBytes == null || headerBytes.Length < 1)
                    {
                        // Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " no byte data read from peer");
                        return null;
                    }

                    #endregion

                    #region Process-Headers-into-Message

                    try
                    {
                        Message = new Message(headerBytes);
                    }
                    catch (Exception e)
                    {
                        Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " exception while reading message headers: " + e.Message);
                        return null;
                    }

                    if (Message == null || Message == default(Message))
                    {
                        Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " null or default message after reading headers");
                        return null;
                    }

                    #endregion

                    #region Check-for-Empty-ContentLength

                    if (Message.ContentLength == null) return Message;
                    if (Message.ContentLength <= 0) return Message;

                    #endregion
                }

                if (ConsoleLogResponseTime) Log("TCPMessageRead read headers " + sw.Elapsed.TotalMilliseconds + "ms");

                #endregion

                #region Read-Data-from-Stream

                using (MemoryStream dataMs = new MemoryStream())
                {
                    long bytesRemaining = Convert.ToInt64(Message.ContentLength);
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

                    if (timeout)
                    {
                        Log("*** TCPMessageRead timeout " + currentTimeout + "ms/" + maxTimeout + "ms exceeded while reading content after reading " + BytesRead + " bytes");
                        return null;
                    }

                    if (ConsoleLogResponseTime) Log("TCPMessageRead read data " + sw.Elapsed.TotalMilliseconds + "ms");

                    contentBytes = dataMs.ToArray();
                    
                    if (ConsoleLogResponseTime) Log("TCPMessageRead data to array " + sw.Elapsed.TotalMilliseconds + "ms");
                }

                #endregion

                #region Check-Content-Bytes-and-Assign

                if (contentBytes == null || contentBytes.Length < 1)
                {
                    Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " no content read");
                    return null;
                }

                if (contentBytes.Length != Message.ContentLength)
                {
                    Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " content length " + contentBytes.Length + " bytes does not match header value of " + Message.ContentLength);
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
                //
                
                return Message;
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return null;
            }
            catch (SocketException SockInner)
            {
                Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return null;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return null;
            }
            catch (AggregateException AEInner)
            {
                Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected (aggregate exception): " + AEInner.Message);
                return null;
            }
            catch (IOException IOInner)
            {
                Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return null;
            }
            catch (Exception EInner)
            {
                Log("*** TCPMessageRead " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("TCPMessageRead " + SourceIp + ":" + SourcePort, EInner);
                return null;
            }
            finally
            {
                sw.Stop();
                if (ConsoleLogResponseTime)
                {
                    if (DataAvailable)
                    {
                        Log("TCPMessageRead " + sw.Elapsed.TotalMilliseconds + "ms");
                    }
                }
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

                if (CurrentClient == null)
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

                SourceIp = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Port;

                CurrentClient.GetStream().Write(Data, 0, Data.Length);
                CurrentClient.GetStream().Flush();
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

                if (CurrentClient == null)
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
                SourceIp = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Port;
                NetworkStream ClientStream = CurrentClient.GetStream();

                #endregion

                #region Read-from-Stream

                if (!IsTCPPeerConnected(CurrentClient))
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

        #endregion

        #region TCP-SSL-Methods

        public static bool TCPSSLMessageWrite(TcpClient CurrentClient, SslStream CurrentSSLStream, Message CurrentMessage, bool ConsoleLogResponseTime)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** TCPSSLMessageWrite null client supplied");
                    return false;
                }

                if (CurrentSSLStream == null)
                {
                    Log("*** TCPSSLMessageWrite null SSL stream supplied");
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("** TCPSSLMessageWrite null message supplied");
                    return false;
                }

                #endregion

                #region Add-Values-if-Needed

                if (CurrentMessage.CreatedUTC == null) CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
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

                //
                // enumerate
                //
                // if (Message.Data != null) Log("TCPSSLMessageWrite " + SourceIp + ":" + SourcePort + " sent " + Message.Data.Length + " content bytes in message of " + MessageAsBytes.Length + " bytes");
                // else Log("TCPSSLMessageWrite " + SourceIp + ":" + SourcePort + " sent (null) content bytes");
                // Log(Message.ToString());
                //
                return true;

                #endregion
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** TCPSSLMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return false;
            }
            catch (SocketException SockInner)
            {
                Log("*** TCPSSLMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return false;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** TCPSSLMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return false;
            }
            catch (IOException IOInner)
            {
                Log("*** TCPSSLMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return false;
            }
            catch (Exception EInner)
            {
                Log("*** TCPSSLMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("TCPSSLMessageWrite " + SourceIp + ":" + SourcePort, EInner);
                return false;
            }
            finally
            {
                sw.Stop();
                if (ConsoleLogResponseTime) Log("TCPSSLMessageWrite " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        public static Message TCPSSLMessageRead(TcpClient CurrentClient, SslStream CurrentSSLStream, bool ConsoleLogResponseTime)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Message Message = new Message();
            bool DataAvailable = false;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** TCPSSLMessageRead null client supplied");
                    return null;
                }

                if (!CurrentClient.Connected)
                {
                    Log("*** TCPSSLMessageRead supplied client is not connected");
                    return null;
                }
                
                #endregion

                #region Variables

                int BytesRead = 0;
                SourceIp = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Port;
                byte[] headerBytes;
                byte[] contentBytes;

                #endregion

                #region Read-Headers-from-Stream

                if (!IsTCPPeerConnected(CurrentClient))
                {
                    Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected while attempting to read headers");
                    return null;
                }

                if (!CurrentSSLStream.CanRead)
                {
                    // Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " stream marked as unreadble while attempting to read headers");
                    // Thread.Sleep(25);
                    return null;
                }
                
                DataAvailable = true;

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
                            /*
                            Log("TCPSSLMessageRead Last four bytes: " + 
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
                                // Log("TCPSSLMessageRead reached end of headers after " + BytesRead + " bytes");
                                break;
                            }
                        }
                    }
                    
                    headerBytes = headerMs.ToArray();
                    if (headerBytes == null || headerBytes.Length < 1)
                    {
                        // Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " no byte data read from peer");
                        return null;
                    }

                    #endregion

                    #region Process-Headers-into-Message

                    try
                    {
                        Message = new Message(headerBytes);
                    }
                    catch (Exception e)
                    {
                        Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " exception while reading message headers: " + e.Message);
                        return null;
                    }

                    if (Message == null || Message == default(Message))
                    {
                        Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " null or default message after reading headers");
                        return null;
                    }

                    #endregion

                    #region Check-for-Empty-ContentLength

                    if (Message.ContentLength == null) return Message;
                    if (Message.ContentLength <= 0) return Message;

                    #endregion
                }

                if (ConsoleLogResponseTime) Log("TCPSSLMessageRead read headers " + sw.Elapsed.TotalMilliseconds + "ms");

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
                        if (BytesRead == Message.ContentLength) break;
                    }
                    
                    if (ConsoleLogResponseTime) Log("TCPSSLMessageRead read data " + sw.Elapsed.TotalMilliseconds + "ms");
                    contentBytes = dataMs.ToArray();
                    if (ConsoleLogResponseTime) Log("TCPSSLMessageRead data to array " + sw.Elapsed.TotalMilliseconds + "ms");
                }

                #endregion

                #region Check-Content-Bytes-and-Assign

                if (contentBytes == null || contentBytes.Length < 1)
                {
                    Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " no content read");
                    return null;
                }

                if (contentBytes.Length != Message.ContentLength)
                {
                    Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " content length " + contentBytes.Length + " bytes does not match header value of " + Message.ContentLength);
                    return null;
                }

                Message.Data = new byte[contentBytes.Length];
                Buffer.BlockCopy(contentBytes, 0, Message.Data, 0, contentBytes.Length);

                #endregion

                //
                // enumerate
                //
                // if (Message.Data != null) Log("TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " returning " + Message.Data.Length + " content bytes");
                // else Log("TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " returning (null) content bytes");
                // Log(Message.ToString());
                //

                return Message;
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return null;
            }
            catch (SocketException SockInner)
            {
                Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return null;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return null;
            }
            catch (AggregateException AEInner)
            {
                Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (aggregate exception): " + AEInner.Message);
                return null;
            }
            catch (IOException IOInner)
            {
                Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return null;
            }
            catch (Exception EInner)
            {
                Log("*** TCPSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("TCPSSLMessageRead " + SourceIp + ":" + SourcePort, EInner);
                return null;
            }
            finally
            {
                sw.Stop();
                if (ConsoleLogResponseTime)
                {
                    if (DataAvailable)
                    {
                        Log("TCPSSLMessageRead " + sw.Elapsed.TotalMilliseconds + "ms");
                    }
                }
            }
        }

        #endregion

        #region Websocket-Methods

        public static async Task<bool> WSMessageWrite(HttpListenerContext CurrentContext, WebSocket CurrentWSClient, Message CurentMessage, bool ConsoleLogResponseTime)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentContext == null)
                {
                    Log("*** WSMessageWrite null context supplied");
                    return false;
                }

                if (CurrentWSClient == null)
                {
                    Log("*** WSMessageWrite null client supplied");
                    return false;
                }

                if (CurentMessage == null)
                {
                    Log("** WSMessageWrite null message supplied");
                    return false;
                }

                #endregion

                #region Add-Values-if-Needed

                if (CurentMessage.CreatedUTC == null) CurentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                if (String.IsNullOrEmpty(CurentMessage.MessageID)) CurentMessage.MessageID = Guid.NewGuid().ToString();

                if (CurentMessage.Data == null || CurentMessage.Data.Length < 1) CurentMessage.ContentLength = 0;
                else CurentMessage.ContentLength = CurentMessage.Data.Length;

                #endregion

                #region Process

                SourceIp = CurrentContext.Request.RemoteEndPoint.Address.ToString();
                SourcePort = CurrentContext.Request.RemoteEndPoint.Port;
                Byte[] MessageAsBytes = CurentMessage.ToBytes();
                await CurrentWSClient.SendAsync(new ArraySegment<byte>(MessageAsBytes, 0, MessageAsBytes.Length), WebSocketMessageType.Binary, true, CancellationToken.None);

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
            finally
            {
                sw.Stop();
                if (ConsoleLogResponseTime)
                {
                    Log("WSMessageWrite " + sw.Elapsed.TotalMilliseconds + "ms");
                }
            }
        }

        public static async Task<Message> WSMessageRead(HttpListenerContext CurrentContext, WebSocket CurrentWSClient, bool ConsoleLogResponseTime)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Message Message = null;
            bool DataAvailable = false;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentContext == null)
                {
                    Log("*** WSMessageRead null context supplied");
                    return null;
                }

                if (CurrentWSClient == null)
                {
                    Log("*** WSMessageRead null client supplied");
                    return null;
                }

                #endregion

                #region Variables

                int BytesRead = 0;
                SourceIp = CurrentContext.Request.RemoteEndPoint.Address.ToString();
                SourcePort = CurrentContext.Request.RemoteEndPoint.Port;

                byte[] headerBytes;
                byte[] contentBytes;

                #endregion

                #region Read-Headers-from-Stream

                if (!IsWSPeerConnected(CurrentWSClient))
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

                    while (CurrentWSClient.State == WebSocketState.Open)
                    {
                        WebSocketReceiveResult receiveResult = await CurrentWSClient.ReceiveAsync(new ArraySegment<byte>(headerBuffer), CancellationToken.None);
                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            await CurrentWSClient.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
                        }

                        DataAvailable = true;

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
                            Log("WSMessageRead Last four bytes: " + 
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
                                // Log("WSMessageRead reached end of headers after " + BytesRead + " bytes");
                                break;
                            }
                        }
                    }

                    headerBytes = headerMs.ToArray();
                    if (headerBytes == null || headerBytes.Length < 1)
                    {
                        // Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " no byte data read from peer");
                        return null;
                    }

                    if (ConsoleLogResponseTime) Log("WSMessageRead read headers " + sw.Elapsed.TotalMilliseconds + "ms");

                    #endregion

                    #region Process-Headers-into-Message

                    try
                    {
                        Message = new Message(headerBytes);
                    }
                    catch (Exception e)
                    {
                        Log("*** WSMessageRead " + SourceIp + ":" + SourcePort + " exception while reading message headers: " + e.Message);
                        return null;
                    }

                    if (Message == null || Message == default(Message))
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

                if (ConsoleLogResponseTime) Log("WSMessageRead read headers " + sw.Elapsed.TotalMilliseconds + "ms");

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

                    if (ConsoleLogResponseTime) Log("WSMessageRead read data " + sw.Elapsed.TotalMilliseconds + "ms");

                    contentBytes = dataMs.ToArray();

                    if (ConsoleLogResponseTime) Log("WSMessageRead data to array " + sw.Elapsed.TotalMilliseconds + "ms");
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
                //

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
            finally
            {
                sw.Stop();
                if (ConsoleLogResponseTime)
                {
                    if (DataAvailable)
                    {
                        Log("WSMessageRead " + sw.Elapsed.TotalMilliseconds + "ms");
                    }
                }
            }
        }

        #endregion

        #region Websocket-SSL-Methods

        public static async Task<bool> WSSSLMessageWrite(HttpListenerContext CurrentContext, WebSocket CurrentWSClient, Message CurentMessage, bool ConsoleLogResponseTime)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentContext == null)
                {
                    Log("*** WSSSLMessageWrite null context supplied");
                    return false;
                }

                if (CurrentWSClient == null)
                {
                    Log("*** WSSSLMessageWrite null client supplied");
                    return false;
                }

                if (CurentMessage == null)
                {
                    Log("** WSSSLMessageWrite null message supplied");
                    return false;
                }

                #endregion

                #region Add-Values-if-Needed

                if (CurentMessage.CreatedUTC == null) CurentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                if (String.IsNullOrEmpty(CurentMessage.MessageID)) CurentMessage.MessageID = Guid.NewGuid().ToString();

                if (CurentMessage.Data == null || CurentMessage.Data.Length < 1) CurentMessage.ContentLength = 0;
                else CurentMessage.ContentLength = CurentMessage.Data.Length;

                #endregion

                #region Process

                SourceIp = CurrentContext.Request.RemoteEndPoint.Address.ToString();
                SourcePort = CurrentContext.Request.RemoteEndPoint.Port;
                Byte[] MessageAsBytes = CurentMessage.ToBytes();
                await CurrentWSClient.SendAsync(new ArraySegment<byte>(MessageAsBytes, 0, MessageAsBytes.Length), WebSocketMessageType.Binary, true, CancellationToken.None);

                //
                // enumerate
                //
                // if (Message.Data != null) Log("WSSSLMessageWrite " + SourceIp + ":" + SourcePort + " sent " + Message.Data.Length + " content bytes in message of " + MessageAsBytes.Length + " bytes");
                // else Log("WSSSLMessageWrite " + SourceIp + ":" + SourcePort + " sent (null) content bytes");
                // Log(Message.ToString());
                //
                return true;

                #endregion
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** WSSSLMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return false;
            }
            catch (SocketException SockInner)
            {
                Log("*** WSSSLMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return false;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** WSSSLMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return false;
            }
            catch (IOException IOInner)
            {
                Log("*** WSSSLMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return false;
            }
            catch (Exception EInner)
            {
                Log("*** WSSSLMessageWrite " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("WSSSLMessageWrite " + SourceIp + ":" + SourcePort, EInner);
                return false;
            }
            finally
            {
                sw.Stop();
                if (ConsoleLogResponseTime)
                {
                    Log("WSSSLMessageWrite " + sw.Elapsed.TotalMilliseconds + "ms");
                }
            }
        }

        public static async Task<Message> WSSSLMessageRead(HttpListenerContext CurrentContext, WebSocket CurrentWSClient, bool ConsoleLogResponseTime)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Message Message = null;
            bool DataAvailable = false;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentContext == null)
                {
                    Log("*** WSSSLMessageRead null context supplied");
                    return null;
                }

                if (CurrentWSClient == null)
                {
                    Log("*** WSSSLMessageRead null client supplied");
                    return null;
                }

                #endregion

                #region Variables

                int BytesRead = 0;
                SourceIp = CurrentContext.Request.RemoteEndPoint.Address.ToString();
                SourcePort = CurrentContext.Request.RemoteEndPoint.Port;

                byte[] headerBytes;
                byte[] contentBytes;

                #endregion

                #region Read-Headers-from-Stream

                if (!IsWSPeerConnected(CurrentWSClient))
                {
                    Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected while attempting to read headers");
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

                    while (CurrentWSClient.State == WebSocketState.Open)
                    {
                        WebSocketReceiveResult receiveResult = await CurrentWSClient.ReceiveAsync(new ArraySegment<byte>(headerBuffer), CancellationToken.None);
                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            await CurrentWSClient.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
                        }

                        DataAvailable = true;

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
                            Log("WSSSLMessageRead Last four bytes: " + 
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
                                // Log("WSSSLMessageRead reached end of headers after " + BytesRead + " bytes");
                                break;
                            }
                        }
                    }

                    headerBytes = headerMs.ToArray();
                    if (headerBytes == null || headerBytes.Length < 1)
                    {
                        // Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " no byte data read from peer");
                        return null;
                    }

                    if (ConsoleLogResponseTime) Log("WSSSLMessageRead read headers " + sw.Elapsed.TotalMilliseconds + "ms");

                    #endregion

                    #region Process-Headers-into-Message

                    try
                    {
                        Message = new Message(headerBytes);
                    }
                    catch (Exception e)
                    {
                        Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " exception while reading message headers: " + e.Message);
                        return null;
                    }

                    if (Message == null || Message == default(Message))
                    {
                        Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " null or default message after reading headers");
                        return null;
                    }

                    #endregion

                    #region Check-for-Empty-ContentLength

                    if (Message.ContentLength == null) return Message;
                    if (Message.ContentLength <= 0) return Message;

                    #endregion
                }

                if (ConsoleLogResponseTime) Log("WSSSLMessageRead read headers " + sw.Elapsed.TotalMilliseconds + "ms");

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

                    if (ConsoleLogResponseTime) Log("WSSSLMessageRead read data " + sw.Elapsed.TotalMilliseconds + "ms");

                    contentBytes = dataMs.ToArray();

                    if (ConsoleLogResponseTime) Log("WSSSLMessageRead data to array " + sw.Elapsed.TotalMilliseconds + "ms");
                }

                #endregion

                #region Check-Content-Bytes-and-Assign

                if (contentBytes == null || contentBytes.Length < 1)
                {
                    Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " no content read");
                    return null;
                }

                if (contentBytes.Length != Message.ContentLength)
                {
                    Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " content length " + contentBytes.Length + " bytes does not match header value of " + Message.ContentLength);
                    return null;
                }

                Message.Data = new byte[contentBytes.Length];
                Buffer.BlockCopy(contentBytes, 0, Message.Data, 0, contentBytes.Length);

                #endregion

                //
                // enumerate
                //
                // if (Message.Data != null) Log("WSSSLMessageRead " + SourceIp + ":" + SourcePort + " returning " + Message.Data.Length + " content bytes");
                // else Log("WSSSLMessageRead " + SourceIp + ":" + SourcePort + " returning (null) content bytes");
                // Log(Message.ToString());
                //

                return Message;
            }
            catch (WebSocketException WSEInner)
            {
                Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (websocket exception): " + WSEInner.Message);
                return null;
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return null;
            }
            catch (SocketException SockInner)
            {
                Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return null;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return null;
            }
            catch (IOException IOInner)
            {
                Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return null;
            }
            catch (Exception EInner)
            {
                Log("*** WSSSLMessageRead " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("WSSSLMessageRead " + SourceIp + ":" + SourcePort, EInner);
                return null;
            }
            finally
            {
                sw.Stop();
                if (ConsoleLogResponseTime)
                {
                    if (DataAvailable)
                    {
                        Log("WSSSLMessageRead " + sw.Elapsed.TotalMilliseconds + "ms");
                    }
                }
            }
        }

        #endregion

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

        public static bool IsTCPPeerConnected(TcpClient CurrentClient)
        {
            // see http://stackoverflow.com/questions/6993295/how-to-determine-if-the-tcp-is-connected-or-not

            bool success = false;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** IsTCPPeerConnected null client supplied");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                success = false;
                SourceIp = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)CurrentClient.Client.RemoteEndPoint).Port;

                if (CurrentClient != null
                    && CurrentClient.Client != null
                    && CurrentClient.Client.Connected)
                {
                    if (CurrentClient.Client.Poll(0, SelectMode.SelectRead))
                    {
                        byte[] buff = new byte[1];
                        if (CurrentClient.Client.Receive(buff, SocketFlags.Peek) == 0) success = false;
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

        public static bool IsWSPeerConnected(WebSocket CurrentWSClient)
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

                if (CurrentWSClient == null)
                {
                    Log("*** IsWSPeerConnected null client supplied");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                if (CurrentWSClient.State == WebSocketState.Open) success = true;
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

            // Newtonsoft
            // Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings();
            // settings.NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore;
            // return (T)Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json, settings);

            // System.Web.Script.Serialization
            JavaScriptSerializer ser = new JavaScriptSerializer();
            ser.MaxJsonLength = Int32.MaxValue;
            return (T)ser.Deserialize<T>(json);
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

            // Newtonsoft
            // Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings();
            // string json = Encoding.UTF8.GetString(bytes);
            // return (T)Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json, settings);

            // System.Web.Script.Serialization
            JavaScriptSerializer ser = new JavaScriptSerializer();
            ser.MaxJsonLength = Int32.MaxValue;
            return (T)ser.Deserialize<T>(Encoding.UTF8.GetString(bytes));
        }
        
        public static string SerializeJson(object obj)
        {
            // Newtonsoft
            // string json = JsonConvert.SerializeObject(obj, Newtonsoft.Json.Formatting.Indented, new JsonSerializerSettings { });
            // return json;

            // System.Web.Script.Serialization
            JavaScriptSerializer ser = new JavaScriptSerializer();
            ser.MaxJsonLength = Int32.MaxValue;
            string json = ser.Serialize(obj);
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
