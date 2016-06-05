using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer
{
	/// <summary>BinaryEditsVisitor implements a binary EditsVisitor</summary>
	public class BinaryEditsVisitor : OfflineEditsVisitor
	{
		private readonly EditLogFileOutputStream elfos;

		/// <summary>Create a processor that writes to a given file</summary>
		/// <param name="outputName">Name of file to write output to</param>
		/// <exception cref="System.IO.IOException"/>
		public BinaryEditsVisitor(string outputName)
		{
			this.elfos = new EditLogFileOutputStream(new Configuration(), new FilePath(outputName
				), 0);
			elfos.Create(NameNodeLayoutVersion.CurrentLayoutVersion);
		}

		/// <summary>Start the visitor (initialization)</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Start(int version)
		{
		}

		/// <summary>Finish the visitor</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close(Exception error)
		{
			elfos.SetReadyToFlush();
			elfos.FlushAndSync(true);
			elfos.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void VisitOp(FSEditLogOp op)
		{
			elfos.Write(op);
		}
	}
}
