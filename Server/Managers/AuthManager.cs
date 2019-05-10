using BigQ.Core;
using SyslogLogging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigQ.Server.Managers
{
    internal class AuthManager : IDisposable
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private bool _Disposed = false;

        private LoggingModule _Logging;
        private ServerConfiguration _Config;

        private string _UsersLastModified;
        private ConcurrentList<User> _Users;
        private CancellationTokenSource _UsersCancellationTokenSource;
        private CancellationToken _UsersCancellationToken;

        private string _PermissionsLastModified;
        private ConcurrentList<Permission> _Permissions;
        private CancellationTokenSource _PermissionsCancellationTokenSource;
        private CancellationToken _PermissionsCancellationToken;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="logging">LoggingModule instance.</param>
        /// <param name="config">ServerConfiguration instance.</param>
        public AuthManager(LoggingModule logging, ServerConfiguration config)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (config == null) throw new ArgumentNullException(nameof(config));

            _Logging = logging;
            _Config = config;

            _UsersCancellationTokenSource = new CancellationTokenSource();
            _UsersCancellationToken = _UsersCancellationTokenSource.Token;
            Task.Run(() => MonitorUsersFile(), _UsersCancellationToken);

            _PermissionsCancellationTokenSource = new CancellationTokenSource();
            _PermissionsCancellationToken = _PermissionsCancellationTokenSource.Token;
            Task.Run(() => MonitorPermissionsFile(), _PermissionsCancellationToken);
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

        /// <summary>
        /// Allow or deny the connection.
        /// </summary>
        /// <param name="email">Email address of the user.</param>
        /// <param name="ip">IP address of the user.</param>
        /// <returns>True if allowed.</returns>
        public bool AllowConnection(string email, string ip)
        {
            try
            {
                if (_Users != null && _Users.Count > 0)
                {
                    #region Check-for-Null-Values

                    if (String.IsNullOrEmpty(email))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AllowConnection no email supplied");
                        return false;
                    }

                    if (String.IsNullOrEmpty(ip))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AllowConnection no IP supplied");
                        return false;
                    }

                    #endregion

                    #region Users-List-Present

                    User currUser = GetUser(email);
                    if (currUser == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AllowConnection unable to find entry for email " + email);
                        return false;
                    }

                    if (String.IsNullOrEmpty(currUser.Permission))
                    {
                        #region No-Permissions-Only-Check-IP

                        if (currUser.IPWhiteList == null || currUser.IPWhiteList.Count < 1)
                        {
                            // deault permit
                            return true;
                        }
                        else
                        {
                            if (currUser.IPWhiteList.Contains(ip)) return true;
                            return false;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Check-Permissions-Object

                        Permission currPermission = GetPermission(currUser.Permission);
                        if (currPermission == null)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "AllowConnection permission entry " + currUser.Permission + " not found for user " + email);
                            return false;
                        }

                        if (!currPermission.Login)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "AllowConnection login permission denied in permission entry " + currUser.Permission + " for user " + email);
                            return false;
                        }

                        #endregion

                        #region Check-IP

                        if (currUser.IPWhiteList == null || currUser.IPWhiteList.Count < 1)
                        {
                            // deault permit
                            return true;
                        }
                        else
                        {
                            if (currUser.IPWhiteList.Contains(ip)) return true;
                            return false;
                        }

                        #endregion
                    }

                    #endregion
                }
                else
                {
                    #region Default-Permit

                    return true;

                    #endregion
                }
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "AllowConnection", e);
                return false;
            }
        }

        /// <summary>
        /// Authorize that a message can be sent.
        /// </summary>
        /// <param name="message">The Message object.</param>
        /// <returns>True if authorized.</returns>
        public bool AuthorizeMessage(Message message)
        {
            try
            {
                #region Check-for-Null-Values

                if (message == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "AuthorizeMessage null message supplied");
                    return false;
                }

                if (_Users == null || _Users.Count < 1)
                {
                    // default permit
                    return true;
                }

                #endregion

                #region Process

                if (!String.IsNullOrEmpty(message.Email))
                {
                    #region Authenticate-Credentials

                    User currUser = GetUser(message.Email);
                    if (currUser == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AuthenticateUser unable to find user " + message.Email);
                        return false;
                    }

                    if (!String.IsNullOrEmpty(currUser.Password))
                    {
                        if (String.Compare(currUser.Password, message.Password) != 0)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "AuthenticateUser invalid password supplied for user " + message.Email);
                            return false;
                        }
                    }

                    #endregion

                    #region Verify-Permissions

                    if (String.IsNullOrEmpty(currUser.Permission))
                    {
                        // default permit
                        // Logging.Log(LoggingModule.Severity.Debug, "AuthenticateUser default permit in use (user " + CurrentMessage.Email + " has null permission list)");
                        return true;
                    }
                     
                    Permission currPermission = GetPermission(currUser.Permission);
                    if (currPermission == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AuthorizeMessage unable to find permission " + currUser.Permission + " for user " + currUser.Email);
                        return false;
                    }

                    if (currPermission.Permissions == null || currPermission.Permissions.Count < 1)
                    {
                        // default permit
                        // Logging.Log(LoggingModule.Severity.Debug, "AuthorizeMessage default permit in use (no permissions found for permission name " + currUser.Permission);
                        return true;
                    }

                    if (currPermission.Permissions.Contains(message.Command.ToString()))
                    {
                        // Logging.Log(LoggingModule.Severity.Debug, "AuthorizeMessage found permission for command " + CurrentMessage.Command + " in permission " + currUser.Permission + " for user " + currUser.Email);
                        return true;
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AuthorizeMessage permission " + currPermission.Name + " does not contain command " + message.Command + " for user " + currUser.Email);
                        return false;
                    }

                    #endregion
                }
                else
                {
                    #region No-Material

                    _Logging.Log(LoggingModule.Severity.Warn, "AuthenticateUser no authentication material supplied");
                    return false;

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "AuthorizeMessage", e);
                return false;
            }
        }

        /// <summary>
        /// Retrieve the current users file.
        /// </summary>
        /// <returns>List of User objects.</returns>
        public List<User> GetCurrentUsersFile()
        {
            if (_Users == null || _Users.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "GetCurrentUsersFile no users listed or no users file");
                return null;
            }

            List<User> ret = new List<User>();
            foreach (User curr in _Users)
            {
                ret.Add(curr);
            }

            _Logging.Log(LoggingModule.Severity.Debug, "GetCurrentUsersFile returning " + ret.Count + " users");
            return ret;
        }

        /// <summary>
        /// Retrieve the current permissions file.
        /// </summary>
        /// <returns>List of Permission objects.</returns>
        public List<Permission> GetCurrentPermissionsFile()
        {
            if (_Permissions == null || _Permissions.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "GetCurrentPermissionsFile no permissions listed or no permissions file");
                return null;
            }

            List<Permission> ret = new List<Permission>();
            foreach (Permission curr in _Permissions)
            {
                ret.Add(curr);
            }

            _Logging.Log(LoggingModule.Severity.Debug, "GetCurrentPermissionsFile returning " + ret.Count + " permissions");
            return ret;
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
                _UsersCancellationTokenSource.Cancel();
                _UsersCancellationTokenSource.Dispose();

                _PermissionsCancellationTokenSource.Cancel();
                _PermissionsCancellationTokenSource.Dispose();
            }

            _Disposed = true;
        }

        private void MonitorUsersFile()
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

                    #region Check-if-Exists

                    if (!File.Exists(_Config.Files.UsersFile))
                    {
                        _Users = new ConcurrentList<User>();
                        continue;
                    }

                    #endregion

                    #region Process

                    string tempTimestamp = "";
                    string fileContents = "";

                    if (String.IsNullOrEmpty(_UsersLastModified))
                    {
                        #region First-Read

                        _Logging.Log(LoggingModule.Severity.Debug, "MonitorUsersFile loading " + _Config.Files.UsersFile);

                        //
                        // get timestamp
                        //
                        _UsersLastModified = File.GetLastWriteTimeUtc(_Config.Files.UsersFile).ToString("MMddyyyy-HHmmss");

                        //
                        // read and store
                        //
                        fileContents = File.ReadAllText(_Config.Files.UsersFile);
                        if (String.IsNullOrEmpty(fileContents))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "MonitorUsersFile empty file found at " + _Config.Files.UsersFile);
                            continue;
                        }

                        try
                        {
                            _Users = Common.DeserializeJson<ConcurrentList<User>>(Encoding.UTF8.GetBytes(fileContents));
                        }
                        catch (Exception EInner)
                        {
                            _Logging.LogException("Server", "MonitorUsersFile", EInner);
                            _Logging.Log(LoggingModule.Severity.Warn, "MonitorUsersFile unable to deserialize contents of " + _Config.Files.UsersFile);
                            continue;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Subsequent-Read

                        //
                        // get timestamp
                        //
                        tempTimestamp = File.GetLastWriteTimeUtc(_Config.Files.UsersFile).ToString("MMddyyyy-HHmmss");

                        //
                        // compare and update
                        //
                        if (String.Compare(_UsersLastModified, tempTimestamp) != 0)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "MonitorUsersFile loading " + _Config.Files.UsersFile);

                            //
                            // get timestamp
                            //
                            _UsersLastModified = File.GetLastWriteTimeUtc(_Config.Files.UsersFile).ToString("MMddyyyy-HHmmss");

                            //
                            // read and store
                            //
                            fileContents = File.ReadAllText(_Config.Files.UsersFile);
                            if (String.IsNullOrEmpty(fileContents))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "MonitorUsersFile empty file found at " + _Config.Files.UsersFile);
                                continue;
                            }

                            try
                            {
                                _Users = Common.DeserializeJson<ConcurrentList<User>>(Encoding.UTF8.GetBytes(fileContents));
                            }
                            catch (Exception EInner)
                            {
                                _Logging.LogException("Server", "MonitorUsersFile", EInner);
                                _Logging.Log(LoggingModule.Severity.Warn, "MonitorUsersFile unable to deserialize contents of " + _Config.Files.UsersFile);
                                continue;
                            }
                        }

                        #endregion
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
                _Logging.LogException("Server", "MonitorUsersFile", e);
            }
        }

        private void MonitorPermissionsFile()
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

                    #region Check-if-Exists

                    if (!File.Exists(_Config.Files.PermissionsFile))
                    {
                        _Permissions = new ConcurrentList<Permission>();
                        continue;
                    }

                    #endregion

                    #region Process

                    string tempTimestamp = "";
                    string fileContents = "";

                    if (String.IsNullOrEmpty(_PermissionsLastModified))
                    {
                        #region First-Read

                        _Logging.Log(LoggingModule.Severity.Debug, "MonitorPermissionsFile loading " + _Config.Files.PermissionsFile);

                        //
                        // get timestamp
                        //
                        _PermissionsLastModified = File.GetLastWriteTimeUtc(_Config.Files.PermissionsFile).ToString("MMddyyyy-HHmmss");

                        //
                        // read and store
                        //
                        fileContents = File.ReadAllText(_Config.Files.PermissionsFile);
                        if (String.IsNullOrEmpty(fileContents))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "MonitorPermissionsFile empty file found at " + _Config.Files.PermissionsFile);
                            continue;
                        }

                        try
                        {
                            _Permissions = Common.DeserializeJson<ConcurrentList<Permission>>(Encoding.UTF8.GetBytes(fileContents));
                        }
                        catch (Exception EInner)
                        {
                            _Logging.LogException("Server", "MonitorPermissionsFile", EInner);
                            _Logging.Log(LoggingModule.Severity.Warn, "MonitorPermissionsFile unable to deserialize contents of " + _Config.Files.PermissionsFile);
                            continue;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Subsequent-Read

                        //
                        // get timestamp
                        //
                        tempTimestamp = File.GetLastWriteTimeUtc(_Config.Files.PermissionsFile).ToString("MMddyyyy-HHmmss");

                        //
                        // compare and update
                        //
                        if (String.Compare(_PermissionsLastModified, tempTimestamp) != 0)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "MonitorPermissionsFile loading " + _Config.Files.PermissionsFile);

                            //
                            // get timestamp
                            //
                            _PermissionsLastModified = File.GetLastWriteTimeUtc(_Config.Files.PermissionsFile).ToString("MMddyyyy-HHmmss");

                            //
                            // read and store
                            //
                            fileContents = File.ReadAllText(_Config.Files.PermissionsFile);
                            if (String.IsNullOrEmpty(fileContents))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "MonitorPermissionsFile empty file found at " + _Config.Files.PermissionsFile);
                                continue;
                            }

                            try
                            {
                                _Permissions = Common.DeserializeJson<ConcurrentList<Permission>>(Encoding.UTF8.GetBytes(fileContents));
                            }
                            catch (Exception EInner)
                            {
                                _Logging.LogException("Server", "MonitorPermissionsFile", EInner);
                                _Logging.Log(LoggingModule.Severity.Warn, "MonitorPermissionsFile unable to deserialize contents of " + _Config.Files.PermissionsFile);
                                continue;
                            }
                        }

                        #endregion
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
                _Logging.LogException("Server", "MonitorPermissionsFile", e);
            }
        }

        private User GetUser(string email)
        {
            try
            {
                #region Check-for-Null-Values

                if (_Users == null || _Users.Count < 1) return null;

                if (String.IsNullOrEmpty(email))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetUser null email supplied");
                    return null;
                }

                #endregion

                #region Process

                foreach (User currUser in _Users)
                {
                    if (String.IsNullOrEmpty(currUser.Email)) continue;
                    if (currUser.Email.ToLower().Equals(email.ToLower()))
                    {
                        return currUser;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Warn, "GetUser unable to find email " + email);
                return null;

                #endregion
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "GetUser", e);
                return null;
            }
        }

        private Permission GetUserPermission(string email)
        {
            try
            {
                #region Check-for-Null-Values

                if (_Permissions == null || _Permissions.Count < 1) return null;
                if (_Users == null || _Users.Count < 1) return null;

                if (String.IsNullOrEmpty(email))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetUserPermissions null email supplied");
                    return null;
                }

                #endregion

                #region Process

                User currUser = GetUser(email);
                if (currUser == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetUserPermission unable to find user " + email);
                    return null;
                }

                if (String.IsNullOrEmpty(currUser.Permission)) return null;
                return GetPermission(currUser.Permission);

                #endregion
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "GetUserPermissions", e);
                return null;
            }
        }

        private Permission GetPermission(string permission)
        {
            try
            {
                #region Check-for-Null-Values

                if (_Permissions == null || _Permissions.Count < 1) return null;
                if (String.IsNullOrEmpty(permission))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetPermission null permission supplied");
                    return null;
                }

                #endregion

                #region Process

                foreach (Permission currPermission in _Permissions)
                {
                    if (String.IsNullOrEmpty(currPermission.Name)) continue;
                    if (permission.ToLower().Equals(currPermission.Name.ToLower()))
                    {
                        return currPermission;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Warn, "GetPermission permission " + permission + " not found");
                return null;

                #endregion
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "GetPermission", e);
                return null;
            }
        }

        #endregion
    }
}
