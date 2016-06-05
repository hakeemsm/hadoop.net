using System.Text;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	/// <summary>
	/// <p><code>JobId</code> represents the <em>globally unique</em>
	/// identifier for a MapReduce job.</p>
	/// <p>The globally unique nature of the identifier is achieved by using the
	/// <em>cluster timestamp</em> from the associated ApplicationId.
	/// </summary>
	/// <remarks>
	/// <p><code>JobId</code> represents the <em>globally unique</em>
	/// identifier for a MapReduce job.</p>
	/// <p>The globally unique nature of the identifier is achieved by using the
	/// <em>cluster timestamp</em> from the associated ApplicationId. i.e.
	/// start-time of the <code>ResourceManager</code> along with a monotonically
	/// increasing counter for the jobId.</p>
	/// </remarks>
	public abstract class JobId : Comparable<JobId>
	{
		/// <summary>
		/// Get the associated <em>ApplicationId</em> which represents the
		/// start time of the <code>ResourceManager</code> and is used to generate
		/// the globally unique <code>JobId</code>.
		/// </summary>
		/// <returns>associated <code>ApplicationId</code></returns>
		public abstract ApplicationId GetAppId();

		/// <summary>
		/// Get the short integer identifier of the <code>JobId</code>
		/// which is unique for all applications started by a particular instance
		/// of the <code>ResourceManager</code>.
		/// </summary>
		/// <returns>short integer identifier of the <code>JobId</code></returns>
		public abstract int GetId();

		public abstract void SetAppId(ApplicationId appId);

		public abstract void SetId(int id);

		protected internal const string Job = "job";

		protected internal const char Separator = '_';

		private sealed class _ThreadLocal_58 : ThreadLocal<NumberFormat>
		{
			public _ThreadLocal_58()
			{
			}

			protected override NumberFormat InitialValue()
			{
				NumberFormat fmt = NumberFormat.GetInstance();
				fmt.SetGroupingUsed(false);
				fmt.SetMinimumIntegerDigits(4);
				return fmt;
			}
		}

		internal static readonly ThreadLocal<NumberFormat> jobIdFormat = new _ThreadLocal_58
			();

		public override string ToString()
		{
			StringBuilder builder = new StringBuilder(Job);
			builder.Append(Separator);
			builder.Append(GetAppId().GetClusterTimestamp());
			builder.Append(Separator);
			builder.Append(jobIdFormat.Get().Format(GetId()));
			return builder.ToString();
		}

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + GetAppId().GetHashCode();
			result = prime * result + GetId();
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			JobId other = (JobId)obj;
			if (!this.GetAppId().Equals(other.GetAppId()))
			{
				return false;
			}
			if (this.GetId() != other.GetId())
			{
				return false;
			}
			return true;
		}

		public virtual int CompareTo(JobId other)
		{
			int appIdComp = this.GetAppId().CompareTo(other.GetAppId());
			if (appIdComp == 0)
			{
				return this.GetId() - other.GetId();
			}
			else
			{
				return appIdComp;
			}
		}
	}
}
