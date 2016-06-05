using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Represents the basic information that is saved per a job when the
	/// JobTracker receives a submitJob request.
	/// </summary>
	/// <remarks>
	/// Represents the basic information that is saved per a job when the
	/// JobTracker receives a submitJob request. The information is saved
	/// so that the JobTracker can recover incomplete jobs upon restart.
	/// </remarks>
	internal class JobInfo : Writable
	{
		private JobID id;

		private Text user;

		private Path jobSubmitDir;

		public JobInfo()
		{
		}

		public JobInfo(JobID id, Text user, Path jobSubmitDir)
		{
			this.id = id;
			this.user = user;
			this.jobSubmitDir = jobSubmitDir;
		}

		/// <summary>Get the job id.</summary>
		public virtual JobID GetJobID()
		{
			return id;
		}

		/// <summary>Get the configured job's user-name.</summary>
		public virtual Text GetUser()
		{
			return user;
		}

		/// <summary>Get the job submission directory</summary>
		public virtual Path GetJobSubmitDir()
		{
			return this.jobSubmitDir;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			id = new JobID();
			id.ReadFields(@in);
			user = new Text();
			user.ReadFields(@in);
			jobSubmitDir = new Path(WritableUtils.ReadString(@in));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			id.Write(@out);
			user.Write(@out);
			WritableUtils.WriteString(@out, jobSubmitDir.ToString());
		}
	}
}
