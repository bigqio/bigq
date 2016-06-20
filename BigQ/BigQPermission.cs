using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    /// <summary>
    /// Object containing information about collections of permissions on BigQ.
    /// </summary>
    [Serializable]
    public class BigQPermission
    {
        #region Class-Variables

        //
        //
        // Standard Headers
        //
        //

        /// <summary>
        /// Name of the permission set.  This is what is referenced in BigQUser's property 'Permission'.
        /// </summary>
        public string Name;

        /// <summary>
        /// Specifies whether users associated with this permission set are able to login to BigQ.
        /// </summary>
        public bool Login;

        /// <summary>
        /// List of strings representing the set of server APIs any client associated with this permission set is able to perform.
        /// </summary>
        public List<string> Permissions;
        
        #endregion

        #region Constructors

        /// <summary>
        /// Do not use.  This is used internally by BigQ libraries.
        /// </summary>
        public BigQPermission()
        {

        }
        
        #endregion

        #region Public-Instance-Methods
        
        /// <summary>
        /// Creates a formatted string containing information about the message.
        /// </summary>
        /// <returns>A formatted string containing information about the message.</returns>
        public override string ToString()
        {
            string ret = "";
            ret += Environment.NewLine;
            ret += " | Name: " + Name + " Login: " + Login + " Permissions: ";
            
            if (Permissions != null && Permissions.Count > 0)
            {
                foreach (string curr in Permissions)
                {
                    ret += curr + " ";
                }
            }
            else
            {
                ret += "<all>";
            }

            ret += Environment.NewLine; // finish off the previous line
            ret += Environment.NewLine;
            return ret;
        }
        
        #endregion

        #region Private-Utility-Methods

        //
        //
        // Not applicable
        //
        //

        #endregion
    }
}
