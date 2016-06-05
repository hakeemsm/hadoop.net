using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer
{
	/// <summary>EditsVisitorFactory for different implementations of EditsVisitor</summary>
	public class OfflineEditsVisitorFactory
	{
		/// <summary>Factory function that creates an EditsVisitor object</summary>
		/// <param name="filename">output filename</param>
		/// <param name="processor">type of visitor to create</param>
		/// <param name="printToScreen">parameter passed to visitor constructor</param>
		/// <returns>EditsVisitor for appropriate output format (binary, xml, etc.)</returns>
		/// <exception cref="System.IO.IOException"/>
		public static OfflineEditsVisitor GetEditsVisitor(string filename, string processor
			, bool printToScreen)
		{
			if (StringUtils.EqualsIgnoreCase("binary", processor))
			{
				return new BinaryEditsVisitor(filename);
			}
			OfflineEditsVisitor vis;
			OutputStream fout = new FileOutputStream(filename);
			OutputStream @out = null;
			try
			{
				if (!printToScreen)
				{
					@out = fout;
				}
				else
				{
					OutputStream[] outs = new OutputStream[2];
					outs[0] = fout;
					outs[1] = System.Console.Out;
					@out = new TeeOutputStream(outs);
				}
				if (StringUtils.EqualsIgnoreCase("xml", processor))
				{
					vis = new XmlEditsVisitor(@out);
				}
				else
				{
					if (StringUtils.EqualsIgnoreCase("stats", processor))
					{
						vis = new StatisticsEditsVisitor(@out);
					}
					else
					{
						throw new IOException("Unknown proccesor " + processor + " (valid processors: xml, binary, stats)"
							);
					}
				}
				@out = fout = null;
				return vis;
			}
			finally
			{
				IOUtils.CloseStream(fout);
				IOUtils.CloseStream(@out);
			}
		}
	}
}
