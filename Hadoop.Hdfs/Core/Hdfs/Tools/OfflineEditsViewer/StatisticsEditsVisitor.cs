using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer
{
	/// <summary>
	/// StatisticsEditsVisitor implements text version of EditsVisitor
	/// that aggregates counts of op codes processed
	/// </summary>
	public class StatisticsEditsVisitor : OfflineEditsVisitor
	{
		private readonly PrintWriter @out;

		private int version = -1;

		private readonly IDictionary<FSEditLogOpCodes, long> opCodeCount = new Dictionary
			<FSEditLogOpCodes, long>();

		/// <summary>
		/// Create a processor that writes to the file named and may or may not
		/// also output to the screen, as specified.
		/// </summary>
		/// <param name="filename">Name of file to write output to</param>
		/// <param name="tokenizer">Input tokenizer</param>
		/// <param name="printToScreen">Mirror output to screen?</param>
		/// <exception cref="System.IO.IOException"/>
		public StatisticsEditsVisitor(OutputStream @out)
		{
			this.@out = new PrintWriter(new OutputStreamWriter(@out, Charsets.Utf8));
		}

		/// <summary>Start the visitor</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Start(int version)
		{
			this.version = version;
		}

		/// <summary>Close the visitor</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close(Exception error)
		{
			@out.Write(GetStatisticsString());
			if (error != null)
			{
				@out.Write("EXITING ON ERROR: " + error.ToString() + "\n");
			}
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void VisitOp(FSEditLogOp op)
		{
			IncrementOpCodeCount(op.opCode);
		}

		/// <summary>Increment the op code counter</summary>
		/// <param name="opCode">opCode for which to increment count</param>
		private void IncrementOpCodeCount(FSEditLogOpCodes opCode)
		{
			if (!opCodeCount.Contains(opCode))
			{
				opCodeCount[opCode] = 0L;
			}
			long newValue = opCodeCount[opCode] + 1;
			opCodeCount[opCode] = newValue;
		}

		/// <summary>Get statistics</summary>
		/// <returns>statistics, map of counts per opCode</returns>
		public virtual IDictionary<FSEditLogOpCodes, long> GetStatistics()
		{
			return opCodeCount;
		}

		/// <summary>Get the statistics in string format, suitable for printing</summary>
		/// <returns>statistics in in string format, suitable for printing</returns>
		public virtual string GetStatisticsString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(string.Format("    %-30.30s      : %d%n", "VERSION", version));
			foreach (FSEditLogOpCodes opCode in FSEditLogOpCodes.Values())
			{
				sb.Append(string.Format("    %-30.30s (%3d): %d%n", opCode.ToString(), opCode.GetOpCode
					(), opCodeCount[opCode]));
			}
			return sb.ToString();
		}
	}
}
