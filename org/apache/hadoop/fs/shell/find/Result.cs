using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	public sealed class Result
	{
		/// <summary>
		/// Result indicating
		/// <see cref="Expression"/>
		/// processing should continue.
		/// </summary>
		public static readonly org.apache.hadoop.fs.shell.find.Result PASS = new org.apache.hadoop.fs.shell.find.Result
			(true, true);

		/// <summary>
		/// Result indicating
		/// <see cref="Expression"/>
		/// processing should stop.
		/// </summary>
		public static readonly org.apache.hadoop.fs.shell.find.Result FAIL = new org.apache.hadoop.fs.shell.find.Result
			(false, true);

		/// <summary>
		/// Result indicating
		/// <see cref="Expression"/>
		/// processing should not descend any more
		/// directories.
		/// </summary>
		public static readonly org.apache.hadoop.fs.shell.find.Result STOP = new org.apache.hadoop.fs.shell.find.Result
			(true, false);

		private bool descend;

		private bool success;

		private Result(bool success, bool recurse)
		{
			this.success = success;
			this.descend = recurse;
		}

		/// <summary>Should further directories be descended.</summary>
		public bool isDescend()
		{
			return this.descend;
		}

		/// <summary>Should processing continue.</summary>
		public bool isPass()
		{
			return this.success;
		}

		/// <summary>Returns the combination of this and another result.</summary>
		public org.apache.hadoop.fs.shell.find.Result combine(org.apache.hadoop.fs.shell.find.Result
			 other)
		{
			return new org.apache.hadoop.fs.shell.find.Result(this.isPass() && other.isPass()
				, this.isDescend() && other.isDescend());
		}

		/// <summary>Negate this result.</summary>
		public org.apache.hadoop.fs.shell.find.Result negate()
		{
			return new org.apache.hadoop.fs.shell.find.Result(!this.isPass(), this.isDescend(
				));
		}

		public override string ToString()
		{
			return "success=" + isPass() + "; recurse=" + isDescend();
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
			if (Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject(
				obj))
			{
				return false;
			}
			org.apache.hadoop.fs.shell.find.Result other = (org.apache.hadoop.fs.shell.find.Result
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
