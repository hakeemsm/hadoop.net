using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	public class HDFSConcat
	{
		private const string def_uri = "hdfs://localhost:9000";

		/// <exception cref="System.IO.IOException"/>
		public static void Main(params string[] args)
		{
			if (args.Length < 2)
			{
				System.Console.Error.WriteLine("Usage HDFSConcat target srcs..");
				System.Environment.Exit(0);
			}
			Configuration conf = new Configuration();
			string uri = conf.Get("fs.default.name", def_uri);
			Path path = new Path(uri);
			DistributedFileSystem dfs = (DistributedFileSystem)FileSystem.Get(path.ToUri(), conf
				);
			Path[] srcs = new Path[args.Length - 1];
			for (int i = 1; i < args.Length; i++)
			{
				srcs[i - 1] = new Path(args[i]);
			}
			dfs.Concat(new Path(args[0]), srcs);
		}
	}
}
