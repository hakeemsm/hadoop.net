using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// Class to encapsulate Queue ACLs for a particular
	/// user.
	/// </summary>
	public class QueueAclsInfo : Writable
	{
		private string queueName;

		private string[] operations;

		/// <summary>Default constructor for QueueAclsInfo.</summary>
		public QueueAclsInfo()
		{
		}

		/// <summary>
		/// Construct a new QueueAclsInfo object using the queue name and the
		/// queue operations array
		/// </summary>
		/// <param name="queueName">Name of the job queue</param>
		/// <param name="operations"/>
		public QueueAclsInfo(string queueName, string[] operations)
		{
			this.queueName = queueName;
			this.operations = operations;
		}

		/// <summary>Get queue name.</summary>
		/// <returns>name</returns>
		public virtual string GetQueueName()
		{
			return queueName;
		}

		protected internal virtual void SetQueueName(string queueName)
		{
			this.queueName = queueName;
		}

		/// <summary>Get opearations allowed on queue.</summary>
		/// <returns>array of String</returns>
		public virtual string[] GetOperations()
		{
			return operations;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			queueName = StringInterner.WeakIntern(Text.ReadString(@in));
			operations = WritableUtils.ReadStringArray(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			Text.WriteString(@out, queueName);
			WritableUtils.WriteStringArray(@out, operations);
		}
	}
}
