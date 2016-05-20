using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>Save the full and short name of the user as a principal.</summary>
	/// <remarks>
	/// Save the full and short name of the user as a principal. This allows us to
	/// have a single type that we always look for when picking up user names.
	/// </remarks>
	internal class User : java.security.Principal
	{
		private readonly string fullName;

		private readonly string shortName;

		private volatile org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 authMethod = null;

		private volatile javax.security.auth.login.LoginContext login = null;

		private volatile long lastLogin = 0;

		public User(string name)
			: this(name, null, null)
		{
		}

		public User(string name, org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 authMethod, javax.security.auth.login.LoginContext login)
		{
			try
			{
				shortName = new org.apache.hadoop.security.HadoopKerberosName(name).getShortName(
					);
			}
			catch (System.IO.IOException ioe)
			{
				throw new System.ArgumentException("Illegal principal name " + name + ": " + ioe.
					ToString(), ioe);
			}
			fullName = name;
			this.authMethod = authMethod;
			this.login = login;
		}

		/// <summary>Get the full name of the user.</summary>
		public virtual string getName()
		{
			return fullName;
		}

		/// <summary>Get the user name up to the first '/' or '@'</summary>
		/// <returns>the leading part of the user name</returns>
		public virtual string getShortName()
		{
			return shortName;
		}

		public override bool Equals(object o)
		{
			if (this == o)
			{
				return true;
			}
			else
			{
				if (o == null || Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject
					(o))
				{
					return false;
				}
				else
				{
					return ((fullName.Equals(((org.apache.hadoop.security.User)o).fullName)) && (authMethod
						 == ((org.apache.hadoop.security.User)o).authMethod));
				}
			}
		}

		public override int GetHashCode()
		{
			return fullName.GetHashCode();
		}

		public override string ToString()
		{
			return fullName;
		}

		public virtual void setAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 authMethod)
		{
			this.authMethod = authMethod;
		}

		public virtual org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 getAuthenticationMethod()
		{
			return authMethod;
		}

		/// <summary>Returns login object</summary>
		/// <returns>login</returns>
		public virtual javax.security.auth.login.LoginContext getLogin()
		{
			return login;
		}

		/// <summary>Set the login object</summary>
		/// <param name="login"/>
		public virtual void setLogin(javax.security.auth.login.LoginContext login)
		{
			this.login = login;
		}

		/// <summary>Set the last login time.</summary>
		/// <param name="time">the number of milliseconds since the beginning of time</param>
		public virtual void setLastLogin(long time)
		{
			lastLogin = time;
		}

		/// <summary>Get the time of the last login.</summary>
		/// <returns>the number of milliseconds since the beginning of time.</returns>
		public virtual long getLastLogin()
		{
			return lastLogin;
		}
	}
}
