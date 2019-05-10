using BigQ.Core;
using SyslogLogging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BigQ.Server.Managers
{
    internal class CleanupManager : IDisposable
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private bool _Disposed = false;

        private LoggingModule _Logging;
        private ServerConfiguration _Config;
        private ConcurrentDictionary<string, DateTime> _ActiveSendMap;

        private CancellationTokenSource _TokenSource;
        private CancellationToken _Token;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="logging">LoggingModule instance.</param>
        /// <param name="config">ServerConfiguration instance.</param>
        /// <param name="activeSendMap">Active send map ConcurrentDictionary instance.</param>
        public CleanupManager(
            LoggingModule logging, 
            ServerConfiguration config, 
            ConcurrentDictionary<string, DateTime> activeSendMap)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (config == null) throw new ArgumentNullException(nameof(config));

            _Logging = logging;
            _Config = config;
            _ActiveSendMap = activeSendMap;

            _TokenSource = new CancellationTokenSource();
            _Token = _TokenSource.Token;

            Task.Run(() => CleanupTask(), _Token);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tear down and dispose of background workers.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region Private-Methods

        protected virtual void Dispose(bool disposing)
        {
            if (_Disposed)
            {
                return;
            }

            if (disposing)
            {
                _TokenSource.Cancel();
                _TokenSource.Dispose();
            }

            _Disposed = true;
        }

        private void CleanupTask()
        {
            try
            {
                bool firstRun = true;

                while (true)
                {
                    #region Wait

                    if (!firstRun)
                    {
                        Task.Delay(5000).Wait();
                    }
                    else
                    {
                        firstRun = false;
                    }

                    #endregion

                    #region Process

                    foreach (KeyValuePair<string, DateTime> curr in _ActiveSendMap)
                    {
                        if (String.IsNullOrEmpty(curr.Key)) continue;
                        if (DateTime.Compare(DateTime.Now.ToUniversalTime(), curr.Value) > 0)
                        {
                            Task.Run(() =>
                            {
                                int elapsed = 0;
                                while (true)
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "CleanupTask attempting to remove active send map for " + curr.Key + " (elapsed " + elapsed + "ms)");
                                    if (!_ActiveSendMap.ContainsKey(curr.Key))
                                    {
                                        _Logging.Log(LoggingModule.Severity.Debug, "CleanupTask key " + curr.Key + " no longer present in active send map, exiting");
                                        break;
                                    }
                                    else
                                    {
                                        DateTime removedVal = DateTime.Now;
                                        if (_ActiveSendMap.TryRemove(curr.Key, out removedVal))
                                        {
                                            _Logging.Log(LoggingModule.Severity.Debug, "CleanupTask key " + curr.Key + " removed by cleanup task, exiting");
                                            break;
                                        }
                                        Task.Delay(1000).Wait();
                                        elapsed += 1000;
                                    }
                                }
                            }, _Token);
                        }
                    }

                    #endregion
                }
            }
            catch (ThreadAbortException)
            {
                // do nothing
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "CleanupTask", e);
            }
        }

        #endregion
    }
}
