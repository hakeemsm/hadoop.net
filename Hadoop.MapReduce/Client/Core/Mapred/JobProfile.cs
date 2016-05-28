using System;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A JobProfile is a MapReduce primitive.</summary>
	/// <remarks>
	/// A JobProfile is a MapReduce primitive.  Tracks a job,
	/// whether living or dead.
	/// </remarks>
	public class JobProfile : Writable
	{
		static JobProfile()
		{
			// register a ctor
			WritableFactories.SetFactory(typeof(Org.Apache.Hadoop.Mapred.JobProfile), new _WritableFactory_45
				());
		}

		private sealed class _WritableFactory_45 : WritableFactory
		{
			public _WritableFactory_45()
			{
			}

			public Writable NewInstance()
			{
				return new Org.Apache.Hadoop.Mapred.JobProfile();
			}
		}

		internal string user;

		internal readonly JobID jobid;

		internal string jobFile;

		internal string url;

		internal string name;

		internal string queueName;

		/// <summary>
		/// Construct an empty
		/// <see cref="JobProfile"/>
		/// .
		/// </summary>
		public JobProfile()
		{
			jobid = new JobID();
		}

		/// <summary>
		/// Construct a
		/// <see cref="JobProfile"/>
		/// the userid, jobid,
		/// job config-file, job-details url and job name.
		/// </summary>
		/// <param name="user">userid of the person who submitted the job.</param>
		/// <param name="jobid">id of the job.</param>
		/// <param name="jobFile">job configuration file.</param>
		/// <param name="url">link to the web-ui for details of the job.</param>
		/// <param name="name">user-specified job name.</param>
		public JobProfile(string user, JobID jobid, string jobFile, string url, string name
			)
			: this(user, jobid, jobFile, url, name, JobConf.DefaultQueueName)
		{
		}

		/// <summary>
		/// Construct a
		/// <see cref="JobProfile"/>
		/// the userid, jobid,
		/// job config-file, job-details url and job name.
		/// </summary>
		/// <param name="user">userid of the person who submitted the job.</param>
		/// <param name="jobid">id of the job.</param>
		/// <param name="jobFile">job configuration file.</param>
		/// <param name="url">link to the web-ui for details of the job.</param>
		/// <param name="name">user-specified job name.</param>
		/// <param name="queueName">name of the queue to which the job is submitted</param>
		public JobProfile(string user, JobID jobid, string jobFile, string url, string name
			, string queueName)
		{
			this.user = user;
			this.jobid = JobID.Downgrade(jobid);
			this.jobFile = jobFile;
			this.url = url;
			this.name = name;
			this.queueName = queueName;
		}

		[System.ObsoleteAttribute(@"use JobProfile(String, JobID, String, String, String) instead"
			)]
		public JobProfile(string user, string jobid, string jobFile, string url, string name
			)
			: this(user, ((JobID)JobID.ForName(jobid)), jobFile, url, name)
		{
		}

		/// <summary>Get the user id.</summary>
		public virtual string GetUser()
		{
			return user;
		}

		/// <summary>Get the job id.</summary>
		public virtual JobID GetJobID()
		{
			return jobid;
		}

		[System.ObsoleteAttribute(@"use getJobID() instead")]
		public virtual string GetJobId()
		{
			return jobid.ToString();
		}

		/// <summary>Get the configuration file for the job.</summary>
		public virtual string GetJobFile()
		{
			return jobFile;
		}

		/// <summary>Get the link to the web-ui for details of the job.</summary>
		public virtual Uri GetURL()
		{
			try
			{
				return new Uri(url);
			}
			catch (IOException)
			{
				return null;
			}
		}

		/// <summary>Get the user-specified job name.</summary>
		public virtual string GetJobName()
		{
			return name;
		}

		/// <summary>Get the name of the queue to which the job is submitted.</summary>
		/// <returns>name of the queue.</returns>
		public virtual string GetQueueName()
		{
			return queueName;
		}

		///////////////////////////////////////
		// Writable
		///////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			jobid.Write(@out);
			Text.WriteString(@out, jobFile);
			Text.WriteString(@out, url);
			Text.WriteString(@out, user);
			Text.WriteString(@out, name);
			Text.WriteString(@out, queueName);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			jobid.ReadFields(@in);
			this.jobFile = StringInterner.WeakIntern(Text.ReadString(@in));
			this.url = StringInterner.WeakIntern(Text.ReadString(@in));
			this.user = StringInterner.WeakIntern(Text.ReadString(@in));
			this.name = StringInterner.WeakIntern(Text.ReadString(@in));
			this.queueName = StringInterner.WeakIntern(Text.ReadString(@in));
		}
	}
}
