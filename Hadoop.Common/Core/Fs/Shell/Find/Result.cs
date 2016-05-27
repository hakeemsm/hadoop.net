using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	public sealed class Result
	{
		/// <summary>
		/// Result indicating
		/// <see cref="Expression"/>
		/// processing should continue.
		/// </summary>
		public static readonly Org.Apache.Hadoop.FS.Shell.Find.Result Pass = new Org.Apache.Hadoop.FS.Shell.Find.Result
			(true, true);

		/// <summary>
		/// Result indicating
		/// <see cref="Expression"/>
		/// processing should stop.
		/// </summary>
		public static readonly Org.Apache.Hadoop.FS.Shell.Find.Result Fail = new Org.Apache.Hadoop.FS.Shell.Find.Result
			(false, true);

		/// <summary>
		/// Result indicating
		/// <see cref="Expression"/>
		/// processing should not descend any more
		/// directories.
		/// </summary>
		public static readonly Org.Apache.Hadoop.FS.Shell.Find.Result Stop = new Org.Apache.Hadoop.FS.Shell.Find.Result
			(true, false);

		private bool descend;

		private bool success;

		private Result(bool success, bool recurse)
		{
			this.success = success;
			this.descend = recurse;
		}

		/// <summary>Should further directories be descended.</summary>
		public bool IsDescend()
		{
			return this.descend;
		}

		/// <summary>Should processing continue.</summary>
		public bool IsPass()
		{
			return this.success;
		}

		/// <summary>Returns the combination of this and another result.</summary>
		public Org.Apache.Hadoop.FS.Shell.Find.Result Combine(Org.Apache.Hadoop.FS.Shell.Find.Result
			 other)
		{
			return new Org.Apache.Hadoop.FS.Shell.Find.Result(this.IsPass() && other.IsPass()
				, this.IsDescend() && other.IsDescend());
		}

		/// <summary>Negate this result.</summary>
		public Org.Apache.Hadoop.FS.Shell.Find.Result Negate()
		{
			return new Org.Apache.Hadoop.FS.Shell.Find.Result(!this.IsPass(), this.IsDescend(
				));
		}

		public override string ToString()
		{
			return "success=" + IsPass() + "; recurse=" + IsDescend();
		}

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + (descend ? 1231 : 1237);
			result = prime * result + (success ? 1231 : 1237);
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
			Org.Apache.Hadoop.FS.Shell.Find.Result other = (Org.Apache.Hadoop.FS.Shell.Find.Result
				)obj;
			if (descend != other.descend)
			{
				return false;
			}
			if (success != other.success)
			{
				return false;
			}
			return true;
		}
	}
}
