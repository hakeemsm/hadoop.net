using System;
using System.IO;
using Javax.Security.Auth.Login;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>Save the full and short name of the user as a principal.</summary>
	/// <remarks>
	/// Save the full and short name of the user as a principal. This allows us to
	/// have a single type that we always look for when picking up user names.
	/// </remarks>
	internal class User : Principal
	{
		private readonly string fullName;

		private readonly string shortName;

		private volatile UserGroupInformation.AuthenticationMethod authMethod = null;

		private volatile LoginContext login = null;

		private volatile long lastLogin = 0;

		public User(string name)
			: this(name, null, null)
		{
		}

		public User(string name, UserGroupInformation.AuthenticationMethod authMethod, LoginContext
			 login)
		{
			try
			{
				shortName = new HadoopKerberosName(name).GetShortName();
			}
			catch (IOException ioe)
			{
				throw new ArgumentException("Illegal principal name " + name + ": " + ioe.ToString
					(), ioe);
			}
			fullName = name;
			this.authMethod = authMethod;
			this.login = login;
		}

		/// <summary>Get the full name of the user.</summary>
		public virtual string GetName()
		{
			return fullName;
		}

		/// <summary>Get the user name up to the first '/' or '@'</summary>
		/// <returns>the leading part of the user name</returns>
		public virtual string GetShortName()
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
				if (o == null || GetType() != o.GetType())
				{
					return false;
				}
				else
				{
					return ((fullName.Equals(((Org.Apache.Hadoop.Security.User)o).fullName)) && (authMethod
						 == ((Org.Apache.Hadoop.Security.User)o).authMethod));
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

		public virtual void SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod
			 authMethod)
		{
			this.authMethod = authMethod;
		}

		public virtual UserGroupInformation.AuthenticationMethod GetAuthenticationMethod(
			)
		{
			return authMethod;
		}

		/// <summary>Returns login object</summary>
		/// <returns>login</returns>
		public virtual LoginContext GetLogin()
		{
			return login;
		}

		/// <summary>Set the login object</summary>
		/// <param name="login"/>
		public virtual void SetLogin(LoginContext login)
		{
			this.login = login;
		}

		/// <summary>Set the last login time.</summary>
		/// <param name="time">the number of milliseconds since the beginning of time</param>
		public virtual void SetLastLogin(long time)
		{
			lastLogin = time;
		}

		/// <summary>Get the time of the last login.</summary>
		/// <returns>the number of milliseconds since the beginning of time.</returns>
		public virtual long GetLastLogin()
		{
			return lastLogin;
		}
	}
}
