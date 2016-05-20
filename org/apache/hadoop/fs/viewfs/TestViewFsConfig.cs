using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	public class TestViewFsConfig
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		public virtual void testInvalidConfig()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/internalDir/linkToDir2", new 
				org.apache.hadoop.fs.Path("file:///dir2").toUri());
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/internalDir/linkToDir2/linkToDir3"
				, new org.apache.hadoop.fs.Path("file:///dir3").toUri());
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

		private sealed class _InodeTree_47 : org.apache.hadoop.fs.viewfs.InodeTree<_T611036023
			>
		{
			public _InodeTree_47(org.apache.hadoop.conf.Configuration baseArg1, string baseArg2
				)
				: base(baseArg1, baseArg2)
			{
			}

			/// <exception cref="java.net.URISyntaxException"/>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			protected internal override _T611036023 getTargetFileSystem(java.net.URI uri)
			{
				return null;
			}

			/// <exception cref="java.net.URISyntaxException"/>
			protected internal override _T611036023 getTargetFileSystem(org.apache.hadoop.fs.viewfs.InodeTree.INodeDir
				<_T611036023> dir)
			{
				return null;
			}

			/// <exception cref="java.net.URISyntaxException"/>
			/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
			protected internal override _T611036023 getTargetFileSystem(java.net.URI[] mergeFsURIList
				)
			{
				return null;
			}
		}
	}
}
