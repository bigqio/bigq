using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using BigQ.Server.Classes;

namespace BigQ.Server.Managers
{
    internal class AuthManager : IDisposable
    {
        #region Public-Members

        #endregion

        #region Private-Members
         
        private ServerConfiguration _Config;

        private string _UsersLastModified;
        private readonly object _UsersLock = new object();
        private List<User> _Users = new List<User>();
        private CancellationTokenSource _UsersCancellationTokenSource;
        private CancellationToken _UsersCancellationToken;

        private string _PermissionsLastModified;
        private readonly object _PermissionsLock = new object();
        private List<Permission> _Permissions = new List<Permission>();
        private CancellationTokenSource _PermissionsCancellationTokenSource;
        private CancellationToken _PermissionsCancellationToken;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="config">ServerConfiguration instance.</param>
        public AuthManager(ServerConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

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
            if (_UsersCancellationTokenSource != null)
            {
                if (!_UsersCancellationTokenSource.IsCancellationRequested) _UsersCancellationTokenSource.Cancel();
                _UsersCancellationTokenSource.Dispose();
                _UsersCancellationTokenSource = null;
            }

            if (_PermissionsCancellationTokenSource != null)
            {
                if (!_PermissionsCancellationTokenSource.IsCancellationRequested) _PermissionsCancellationTokenSource.Cancel();
                _PermissionsCancellationTokenSource.Dispose();
                _PermissionsCancellationTokenSource = null;
            }

            _Users = null;
            _Permissions = null;
            _Config = null;
        }

        /// <summary>
        /// Allow or deny the connection.
        /// </summary>
        /// <param name="email">Email address of the user.</param>
        /// <param name="ip">IP address of the user.</param>
        /// <returns>True if allowed.</returns>
        public bool AllowConnection(string email, string ip)
        { 
            if (_Users != null && _Users.Count > 0)
            { 
                if (String.IsNullOrEmpty(email)) return false;
                if (String.IsNullOrEmpty(ip)) return false;
                      
                User currUser = GetUser(email);
                if (currUser == null || currUser == default(User)) return false;

                if (String.IsNullOrEmpty(currUser.Permission))
                {
                    #region No-Permissions-Only-Check-IP

                    if (currUser.IPWhiteList == null || currUser.IPWhiteList.Count < 1)
                    { 
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
                    if (currPermission == null || currPermission == default(Permission)) return false;
                    if (!currPermission.Login) return false;

                    #endregion

                    #region Check-IP

                    if (currUser.IPWhiteList == null || currUser.IPWhiteList.Count < 1) return true;
                    else
                    {
                        if (currUser.IPWhiteList.Contains(ip)) return true;
                        return false;
                    }

                    #endregion
                } 
            }
            else
            {
                return true; // default permit
            } 
        }

        /// <summary>
        /// Authorize that a message can be sent.
        /// </summary>
        /// <param name="message">The Message object.</param>
        /// <returns>True if authorized.</returns>
        public bool AuthorizeMessage(Message message)
        { 
            if (message == null) return false;
            if (_Users == null || _Users.Count < 1) return true;
                  
            if (!String.IsNullOrEmpty(message.Email))
            {
                #region Authenticate-Credentials

                User currUser = GetUser(message.Email);
                if (currUser == null || currUser == default(User)) return false;
                if (!String.IsNullOrEmpty(currUser.Password)) 
                {
                    if (String.Compare(currUser.Password, message.Password) != 0) return false;
                }

                #endregion

                #region Verify-Permissions

                if (String.IsNullOrEmpty(currUser.Permission)) return true; // default permit
                Permission currPermission = GetPermission(currUser.Permission);
                if (currPermission == null || currPermission == default(Permission)) return false;
                if (currPermission.Permissions == null || currPermission.Permissions.Count < 1) return true; // default permit
                if (currPermission.Permissions.Contains(message.Command.ToString())) return true;
                return false;

                #endregion
            }
            else
            {
                return false; // no material
            } 
        }

        /// <summary>
        /// Retrieve the current users file.
        /// </summary>
        /// <returns>List of User objects.</returns>
        public List<User> GetCurrentUsersFile()
        {
            lock (_UsersLock)
            {
                if (_Users == null || _Users.Count < 1) return null;
                List<User> ret = new List<User>(_Users);
                return ret;
            }
        }

        /// <summary>
        /// Retrieve the current permissions file.
        /// </summary>
        /// <returns>List of Permission objects.</returns>
        public List<Permission> GetCurrentPermissionsFile()
        {
            lock (_PermissionsLock)
            {
                if (_Permissions == null || _Permissions.Count < 1) return null;
                return new List<Permission>(_Permissions);
            }
        }

        #endregion

        #region Private-Methods
         
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
                     
                    #region Process

                    string tempTimestamp = "";
                    string fileContents = "";

                    if (String.IsNullOrEmpty(_UsersLastModified))
                    {
                        #region First-Read
                         
                        //
                        // get timestamp
                        //
                        _UsersLastModified = File.GetLastWriteTimeUtc(_Config.Files.UsersFile).ToString("MMddyyyy-HHmmss");

                        //
                        // read and store
                        //
                        fileContents = File.ReadAllText(_Config.Files.UsersFile);
                        if (String.IsNullOrEmpty(fileContents)) continue;

                        try
                        {
                            lock (_UsersLock)
                            {
                                _Users = Common.DeserializeJson<List<User>>(Encoding.UTF8.GetBytes(fileContents));
                            }
                        }
                        catch (Exception)
                        {
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
                            //
                            // get timestamp
                            //
                            _UsersLastModified = File.GetLastWriteTimeUtc(_Config.Files.UsersFile).ToString("MMddyyyy-HHmmss");

                            //
                            // read and store
                            //
                            fileContents = File.ReadAllText(_Config.Files.UsersFile);
                            if (String.IsNullOrEmpty(fileContents)) continue;

                            try
                            {
                                lock (_UsersLock)
                                {
                                    _Users = Common.DeserializeJson<List<User>>(Encoding.UTF8.GetBytes(fileContents));
                                }
                            }
                            catch (Exception)
                            { 
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
            catch (Exception)
            {

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

                    if (!File.Exists(_Config.Files.PermissionsFile)) continue;

                    #endregion

                    #region Process

                    string tempTimestamp = "";
                    string fileContents = "";

                    if (String.IsNullOrEmpty(_PermissionsLastModified))
                    {
                        #region First-Read
                         
                        //
                        // get timestamp
                        //
                        _PermissionsLastModified = File.GetLastWriteTimeUtc(_Config.Files.PermissionsFile).ToString("MMddyyyy-HHmmss");

                        //
                        // read and store
                        //
                        fileContents = File.ReadAllText(_Config.Files.PermissionsFile);
                        if (String.IsNullOrEmpty(fileContents)) continue;

                        try
                        {
                            lock (_PermissionsLock)
                            {
                                _Permissions = Common.DeserializeJson<List<Permission>>(Encoding.UTF8.GetBytes(fileContents));
                            }
                        }
                        catch (Exception)
                        { 
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
                            //
                            // get timestamp
                            //
                            _PermissionsLastModified = File.GetLastWriteTimeUtc(_Config.Files.PermissionsFile).ToString("MMddyyyy-HHmmss");

                            //
                            // read and store
                            //
                            fileContents = File.ReadAllText(_Config.Files.PermissionsFile);
                            if (String.IsNullOrEmpty(fileContents)) continue;

                            try
                            {
                                lock (_PermissionsLock)
                                {
                                    _Permissions = Common.DeserializeJson<List<Permission>>(Encoding.UTF8.GetBytes(fileContents));
                                }
                            }
                            catch (Exception)
                            { 
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
            catch (Exception)
            {

            }
        }

        private User GetUser(string email)
        {
            if (String.IsNullOrEmpty(email)) return null;

            lock (_UsersLock)
            { 
                if (_Users == null || _Users.Count < 1) return null; 
                User ret = _Users.FirstOrDefault(u => u.Email.ToLower().Equals(email.ToLower()));
                if (ret == default(User)) return null;
                return ret;
            }
        }

        private Permission GetUserPermission(string email)
        {
            if (String.IsNullOrEmpty(email)) return null;
            Permission ret = null;

            lock (_PermissionsLock)
            {
                if (_Permissions == null || _Permissions.Count < 1) return null; 
                  
                User currUser = GetUser(email);
                if (currUser == null) return null;
                if (String.IsNullOrEmpty(currUser.Permission)) return null;

                ret = _Permissions.FirstOrDefault(p => p.Name.ToLower().Equals(currUser.Permission.ToLower()));
                if (ret == default(Permission)) return null;
                return ret;
            }
        }

        private Permission GetPermission(string permission)
        {
            if (String.IsNullOrEmpty(permission)) return null;

            lock (_PermissionsLock)
            { 
                if (_Permissions == null || _Permissions.Count < 1) return null;

                Permission ret = _Permissions.FirstOrDefault(p => p.Name.ToLower().Equals(permission.ToLower()));
                if (ret == default(Permission)) return null;
                return ret;
            }
        }

        #endregion
    }
}
