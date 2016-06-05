using System;
using System.IO;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// JobID represents the immutable and unique identifier for
	/// the job.
	/// </summary>
	/// <remarks>
	/// JobID represents the immutable and unique identifier for
	/// the job. JobID consists of two parts. First part
	/// represents the jobtracker identifier, so that jobID to jobtracker map
	/// is defined. For cluster setup this string is the jobtracker
	/// start time, for local setting, it is "local".
	/// Second part of the JobID is the job number. <br />
	/// An example JobID is :
	/// <code>job_200707121733_0003</code> , which represents the third job
	/// running at the jobtracker started at <code>200707121733</code>.
	/// <p>
	/// Applications should never construct or parse JobID strings, but rather
	/// use appropriate constructors or
	/// <see cref="ForName(string)"/>
	/// method.
	/// </remarks>
	/// <seealso cref="TaskID"/>
	/// <seealso cref="TaskAttemptID"/>
	public class JobID : Org.Apache.Hadoop.Mapreduce.JobID
	{
		/// <summary>Constructs a JobID object</summary>
		/// <param name="jtIdentifier">jobTracker identifier</param>
		/// <param name="id">job number</param>
		public JobID(string jtIdentifier, int id)
			: base(jtIdentifier, id)
		{
		}

		public JobID()
		{
		}

		/// <summary>Downgrade a new JobID to an old one</summary>
		/// <param name="old">a new or old JobID</param>
		/// <returns>either old or a new JobID build to match old</returns>
		public static Org.Apache.Hadoop.Mapred.JobID Downgrade(Org.Apache.Hadoop.Mapreduce.JobID
			 old)
		{
			if (old is Org.Apache.Hadoop.Mapred.JobID)
			{
				return (Org.Apache.Hadoop.Mapred.JobID)old;
			}
			else
			{
				return new Org.Apache.Hadoop.Mapred.JobID(old.GetJtIdentifier(), old.GetId());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public static Org.Apache.Hadoop.Mapred.JobID Read(DataInput @in)
		{
			Org.Apache.Hadoop.Mapred.JobID jobId = new Org.Apache.Hadoop.Mapred.JobID();
			jobId.ReadFields(@in);
			return jobId;
		}

		/// <summary>Construct a JobId object from given string</summary>
		/// <returns>constructed JobId object or null if the given String is null</returns>
		/// <exception cref="System.ArgumentException">if the given string is malformed</exception>
		public static Org.Apache.Hadoop.Mapreduce.JobID ForName(string str)
		{
			return (Org.Apache.Hadoop.Mapred.JobID)Org.Apache.Hadoop.Mapreduce.JobID.ForName(
				str);
		}

		/// <summary>Returns a regex pattern which matches task IDs.</summary>
		/// <remarks>
		/// Returns a regex pattern which matches task IDs. Arguments can
		/// be given null, in which case that part of the regex will be generic.
		/// For example to obtain a regex matching <i>any job</i>
		/// run on the jobtracker started at <i>200707121733</i>, we would use :
		/// <pre>
		/// JobID.getTaskIDsPattern("200707121733", null);
		/// </pre>
		/// which will return :
		/// <pre> "job_200707121733_[0-9]*" </pre>
		/// </remarks>
		/// <param name="jtIdentifier">jobTracker identifier, or null</param>
		/// <param name="jobId">job number, or null</param>
		/// <returns>a regex pattern matching JobIDs</returns>
		[Obsolete]
		public static string GetJobIDsPattern(string jtIdentifier, int jobId)
		{
			StringBuilder builder = new StringBuilder(Job).Append(Separator);
			builder.Append(GetJobIDsPatternWOPrefix(jtIdentifier, jobId));
			return builder.ToString();
		}

		[Obsolete]
		internal static StringBuilder GetJobIDsPatternWOPrefix(string jtIdentifier, int jobId
			)
		{
			StringBuilder builder = new StringBuilder();
			if (jtIdentifier != null)
			{
				builder.Append(jtIdentifier);
			}
			else
			{
				builder.Append("[^").Append(Separator).Append("]*");
			}
			builder.Append(Separator).Append(jobId != null ? idFormat.Format(jobId) : "[0-9]*"
				);
			return builder;
		}
	}
}
