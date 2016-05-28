using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>Information about TaskTracker.</summary>
	public class TaskTrackerInfo : Writable
	{
		internal string name;

		internal bool isBlacklisted = false;

		internal string reasonForBlacklist = string.Empty;

		internal string blacklistReport = string.Empty;

		public TaskTrackerInfo()
		{
		}

		public TaskTrackerInfo(string name)
		{
			// construct an active tracker
			this.name = name;
		}

		public TaskTrackerInfo(string name, string reasonForBlacklist, string report)
		{
			// construct blacklisted tracker
			this.name = name;
			this.isBlacklisted = true;
			this.reasonForBlacklist = reasonForBlacklist;
			this.blacklistReport = report;
		}

		/// <summary>Gets the tasktracker's name.</summary>
		/// <returns>tracker's name.</returns>
		public virtual string GetTaskTrackerName()
		{
			return name;
		}

		/// <summary>Whether tracker is blacklisted</summary>
		/// <returns>
		/// true if tracker is blacklisted
		/// false otherwise
		/// </returns>
		public virtual bool IsBlacklisted()
		{
			return isBlacklisted;
		}

		/// <summary>Gets the reason for which the tasktracker was blacklisted.</summary>
		/// <returns>reason which tracker was blacklisted</returns>
		public virtual string GetReasonForBlacklist()
		{
			return reasonForBlacklist;
		}

		/// <summary>Gets a descriptive report about why the tasktracker was blacklisted.</summary>
		/// <returns>report describing why the tasktracker was blacklisted.</returns>
		public virtual string GetBlacklistReport()
		{
			return blacklistReport;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			name = Text.ReadString(@in);
			isBlacklisted = @in.ReadBoolean();
			reasonForBlacklist = Text.ReadString(@in);
			blacklistReport = Text.ReadString(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			Text.WriteString(@out, name);
			@out.WriteBoolean(isBlacklisted);
			Text.WriteString(@out, reasonForBlacklist);
			Text.WriteString(@out, blacklistReport);
		}
	}
}
