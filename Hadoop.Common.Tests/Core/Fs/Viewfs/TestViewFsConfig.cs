using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestViewFsConfig
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public virtual void TestInvalidConfig()
		{
			Configuration conf = new Configuration();
			ConfigUtil.AddLink(conf, "/internalDir/linkToDir2", new Path("file:///dir2").ToUri
				());
			ConfigUtil.AddLink(conf, "/internalDir/linkToDir2/linkToDir3", new Path("file:///dir3"
				).ToUri());
			new _InodeTree_47(conf, null);
		}

		internal class _T611036023
		{
			internal _T611036023(TestViewFsConfig _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestViewFsConfig _enclosing;
		}

		private sealed class _InodeTree_47 : InodeTree<_T611036023>
		{
			public _InodeTree_47(Configuration baseArg1, string baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			/// <exception cref="Sharpen.URISyntaxException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
			protected internal override _T611036023 GetTargetFileSystem(URI uri)
			{
				return null;
			}

			/// <exception cref="Sharpen.URISyntaxException"/>
			protected internal override _T611036023 GetTargetFileSystem(InodeTree.INodeDir<_T611036023
				> dir)
			{
				return null;
			}

			/// <exception cref="Sharpen.URISyntaxException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
			protected internal override _T611036023 GetTargetFileSystem(URI[] mergeFsURIList)
			{
				return null;
			}
		}
	}
}
