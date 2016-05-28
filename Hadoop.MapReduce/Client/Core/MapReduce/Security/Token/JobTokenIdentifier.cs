using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security.Token
{
	/// <summary>The token identifier for job token</summary>
	public class JobTokenIdentifier : TokenIdentifier
	{
		private Text jobid;

		public static readonly Text KindName = new Text("mapreduce.job");

		/// <summary>Default constructor</summary>
		public JobTokenIdentifier()
		{
			this.jobid = new Text();
		}

		/// <summary>Create a job token identifier from a jobid</summary>
		/// <param name="jobid">the jobid to use</param>
		public JobTokenIdentifier(Text jobid)
		{
			this.jobid = jobid;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override Text GetKind()
		{
			return KindName;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override UserGroupInformation GetUser()
		{
			if (jobid == null || string.Empty.Equals(jobid.ToString()))
			{
				return null;
			}
			return UserGroupInformation.CreateRemoteUser(jobid.ToString());
		}

		/// <summary>Get the jobid</summary>
		/// <returns>the jobid</returns>
		public virtual Text GetJobId()
		{
			return jobid;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			jobid.ReadFields(@in);
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			jobid.Write(@out);
		}

		public class Renewer : Token.TrivialRenewer
		{
			protected override Text GetKind()
			{
				return KindName;
			}
		}
	}
}
