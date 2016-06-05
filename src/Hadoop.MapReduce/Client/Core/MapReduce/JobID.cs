using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
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
	/// start time, for local setting, it is "local" and a random number.
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
	public class JobID : ID, Comparable<ID>
	{
		public const string Job = "job";

		public const string JobidRegex = Job + Separator + "[0-9]+" + Separator + "[0-9]+";

		private readonly Text jtIdentifier;

		protected internal static readonly NumberFormat idFormat = NumberFormat.GetInstance
			();

		static JobID()
		{
			// Jobid regex for various tools and framework components
			idFormat.SetGroupingUsed(false);
			idFormat.SetMinimumIntegerDigits(4);
		}

		/// <summary>Constructs a JobID object</summary>
		/// <param name="jtIdentifier">jobTracker identifier</param>
		/// <param name="id">job number</param>
		public JobID(string jtIdentifier, int id)
			: base(id)
		{
			this.jtIdentifier = new Text(jtIdentifier);
		}

		public JobID()
		{
			jtIdentifier = new Text();
		}

		public virtual string GetJtIdentifier()
		{
			return jtIdentifier.ToString();
		}

		public override bool Equals(object o)
		{
			if (!base.Equals(o))
			{
				return false;
			}
			Org.Apache.Hadoop.Mapreduce.JobID that = (Org.Apache.Hadoop.Mapreduce.JobID)o;
			return this.jtIdentifier.Equals(that.jtIdentifier);
		}

		/// <summary>Compare JobIds by first jtIdentifiers, then by job numbers</summary>
		public override int CompareTo(ID o)
		{
			Org.Apache.Hadoop.Mapreduce.JobID that = (Org.Apache.Hadoop.Mapreduce.JobID)o;
			int jtComp = this.jtIdentifier.CompareTo(that.jtIdentifier);
			if (jtComp == 0)
			{
				return this.id - that.id;
			}
			else
			{
				return jtComp;
			}
		}

		/// <summary>Add the stuff after the "job" prefix to the given builder.</summary>
		/// <remarks>
		/// Add the stuff after the "job" prefix to the given builder. This is useful,
		/// because the sub-ids use this substring at the start of their string.
		/// </remarks>
		/// <param name="builder">the builder to append to</param>
		/// <returns>the builder that was passed in</returns>
		public virtual StringBuilder AppendTo(StringBuilder builder)
		{
			builder.Append(Separator);
			builder.Append(jtIdentifier);
			builder.Append(Separator);
			builder.Append(idFormat.Format(id));
			return builder;
		}

		public override int GetHashCode()
		{
			return jtIdentifier.GetHashCode() + id;
		}

		public override string ToString()
		{
			return AppendTo(new StringBuilder(Job)).ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			base.ReadFields(@in);
			this.jtIdentifier.ReadFields(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			base.Write(@out);
			jtIdentifier.Write(@out);
		}

		/// <summary>Construct a JobId object from given string</summary>
		/// <returns>constructed JobId object or null if the given String is null</returns>
		/// <exception cref="System.ArgumentException">if the given string is malformed</exception>
		public static Org.Apache.Hadoop.Mapreduce.JobID ForName(string str)
		{
			if (str == null)
			{
				return null;
			}
			try
			{
				string[] parts = str.Split("_");
				if (parts.Length == 3)
				{
					if (parts[0].Equals(Job))
					{
						return new Org.Apache.Hadoop.Mapred.JobID(parts[1], System.Convert.ToInt32(parts[
							2]));
					}
				}
			}
			catch (Exception)
			{
			}
			//fall below
			throw new ArgumentException("JobId string : " + str + " is not properly formed");
		}
	}
}
