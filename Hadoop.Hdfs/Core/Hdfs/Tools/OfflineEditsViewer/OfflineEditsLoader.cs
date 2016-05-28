using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer
{
	/// <summary>OfflineEditsLoader walks an EditsVisitor over an EditLogInputStream</summary>
	internal abstract class OfflineEditsLoader
	{
		/// <exception cref="System.IO.IOException"/>
		public abstract void LoadEdits();

		public class OfflineEditsLoaderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			internal static OfflineEditsLoader CreateLoader(OfflineEditsVisitor visitor, string
				 inputFileName, bool xmlInput, OfflineEditsViewer.Flags flags)
			{
				if (xmlInput)
				{
					return new OfflineEditsXmlLoader(visitor, new FilePath(inputFileName), flags);
				}
				else
				{
					FilePath file = null;
					EditLogInputStream elis = null;
					OfflineEditsLoader loader = null;
					try
					{
						file = new FilePath(inputFileName);
						elis = new EditLogFileInputStream(file, HdfsConstants.InvalidTxid, HdfsConstants.
							InvalidTxid, false);
						loader = new OfflineEditsBinaryLoader(visitor, elis, flags);
					}
					finally
					{
						if ((loader == null) && (elis != null))
						{
							elis.Close();
						}
					}
					return loader;
				}
			}
		}
	}

	internal static class OfflineEditsLoaderConstants
	{
	}
}
