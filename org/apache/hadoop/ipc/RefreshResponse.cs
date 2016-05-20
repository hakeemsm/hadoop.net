using Sharpen;

namespace org.apache.hadoop.ipc
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
		public static org.apache.hadoop.ipc.RefreshResponse successResponse()
		{
			return new org.apache.hadoop.ipc.RefreshResponse(0, "Success");
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
		public virtual void setSenderName(string name)
		{
			senderName = name;
		}

		public virtual string getSenderName()
		{
			return senderName;
		}

		public virtual int getReturnCode()
		{
			return returnCode;
		}

		public virtual void setReturnCode(int rc)
		{
			returnCode = rc;
		}

		public virtual void setMessage(string m)
		{
			message = m;
		}

		public virtual string getMessage()
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
