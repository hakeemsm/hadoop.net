

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Return a response in the handler method for the user to see.</summary>
	/// <remarks>
	/// Return a response in the handler method for the user to see.
	/// Useful since you may want to display status to a user even though an
	/// error has not occurred.
	/// </remarks>
	public class RefreshResponse
	{
		private int returnCode = -1;

		private string message;

		private string senderName;

		/// <summary>Convenience method to create a response for successful refreshes.</summary>
		/// <returns>void response</returns>
		public static Org.Apache.Hadoop.Ipc.RefreshResponse SuccessResponse()
		{
			return new Org.Apache.Hadoop.Ipc.RefreshResponse(0, "Success");
		}

		public RefreshResponse(int returnCode, string message)
		{
			// Most RefreshHandlers will use this
			this.returnCode = returnCode;
			this.message = message;
		}

		/// <summary>Optionally set the sender of this RefreshResponse.</summary>
		/// <remarks>
		/// Optionally set the sender of this RefreshResponse.
		/// This helps clarify things when multiple handlers respond.
		/// </remarks>
		/// <param name="name">The name of the sender</param>
		public virtual void SetSenderName(string name)
		{
			senderName = name;
		}

		public virtual string GetSenderName()
		{
			return senderName;
		}

		public virtual int GetReturnCode()
		{
			return returnCode;
		}

		public virtual void SetReturnCode(int rc)
		{
			returnCode = rc;
		}

		public virtual void SetMessage(string m)
		{
			message = m;
		}

		public virtual string GetMessage()
		{
			return message;
		}

		public override string ToString()
		{
			string ret = string.Empty;
			if (senderName != null)
			{
				ret += senderName + ": ";
			}
			if (message != null)
			{
				ret += message;
			}
			ret += " (exit " + returnCode + ")";
			return ret;
		}
	}
}
