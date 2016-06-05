using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	internal class JvmContext : Writable
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.JvmContext
			));

		internal JVMId jvmId;

		internal string pid;

		internal JvmContext()
		{
			jvmId = new JVMId();
			pid = string.Empty;
		}

		internal JvmContext(JVMId id, string pid)
		{
			jvmId = id;
			this.pid = pid;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			jvmId.ReadFields(@in);
			this.pid = Text.ReadString(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			jvmId.Write(@out);
			Text.WriteString(@out, pid);
		}
	}
}
