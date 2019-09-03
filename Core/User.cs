using System;
using System.Collections.Generic;

namespace BigQ.Core
{
    /// <summary>
    /// Object containing metadata about a user on BigQ.
    /// </summary>
    [Serializable]
    public class User
    {
        #region Public-Members
         
        /// <summary>
        /// Email address of the client.  Primarily used in authentication (future).
        /// </summary>
        public string Email;

        /// <summary>
        /// Password of the client.  Primarily used in authentication (future).
        /// </summary>
        public string Password;

        /// <summary>
        /// Unmanaged string field to store notes about this user.
        /// </summary>
        public string Notes;

        /// <summary>
        /// Specifies the permission group to which the user shall be associated.
        /// </summary>
        public string Permission;

        /// <summary>
        /// List of strings containing allowed IP addresses and subnets (all are permitted if empty or null).
        /// </summary>
        public List<string> IPWhiteList;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Do not use.  This is used internally by BigQ libraries.
        /// </summary>
        public User()
        {

        }
        
        #endregion

        #region Public-Methods
        
        /// <summary>
        /// Creates a formatted string containing information about the message.
        /// </summary>
        /// <returns>A formatted string containing information about the message.</returns>
        public override string ToString()
        {
            string ret = "";
            ret += Environment.NewLine;
            
            if (!String.IsNullOrEmpty(Email)
                || !String.IsNullOrEmpty(Permission)
                || !String.IsNullOrEmpty(Notes)
                )
            {
                ret += " | ";
                if (!String.IsNullOrEmpty(Email)) ret += "Email " + Email + " ";
                if (!String.IsNullOrEmpty(Permission)) ret += "Permission " + Permission + " ";
                if (!String.IsNullOrEmpty(Notes)) ret += "Notes " + Notes + " ";
                ret += Environment.NewLine;
            }
            
            if (IPWhiteList != null && IPWhiteList.Count > 0)
            {
                ret += " | IPWhiteList: ";
                foreach (string curr in IPWhiteList)
                {
                    ret += curr + " ";
                }

                ret += Environment.NewLine;
            }
            else
            {
                ret += " | IPWhiteList: <any>" + Environment.NewLine;
            }

            ret += Environment.NewLine;
            return ret;
        }
        
        #endregion

        #region Private-Methods
        
        #endregion
    }
}
