using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDFSShellGenericOptions
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDFSCommand()
		{
			string namenode = null;
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).Build();
				namenode = FileSystem.GetDefaultUri(conf).ToString();
				string[] args = new string[4];
				args[2] = "-mkdir";
				args[3] = "/data";
				TestFsOption(args, namenode);
				TestConfOption(args, namenode);
				TestPropertyOption(args, namenode);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private void TestFsOption(string[] args, string namenode)
		{
			// prepare arguments to create a directory /data
			args[0] = "-fs";
			args[1] = namenode;
			Execute(args, namenode);
		}

		private void TestConfOption(string[] args, string namenode)
		{
			// prepare configuration hdfs-site.xml
			FilePath configDir = new FilePath(new FilePath("build", "test"), "minidfs");
			NUnit.Framework.Assert.IsTrue(configDir.Mkdirs());
			FilePath siteFile = new FilePath(configDir, "hdfs-site.xml");
			PrintWriter pw;
			try
			{
				pw = new PrintWriter(siteFile);
				pw.Write("<?xml version=\"1.0\"?>\n" + "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"
					 + "<configuration>\n" + " <property>\n" + "   <name>fs.defaultFS</name>\n" + "   <value>"
					 + namenode + "</value>\n" + " </property>\n" + "</configuration>\n");
				pw.Close();
				// prepare arguments to create a directory /data
				args[0] = "-conf";
				args[1] = siteFile.GetPath();
				Execute(args, namenode);
			}
			catch (FileNotFoundException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
			finally
			{
				siteFile.Delete();
				configDir.Delete();
			}
		}

		private void TestPropertyOption(string[] args, string namenode)
		{
			// prepare arguments to create a directory /data
			args[0] = "-D";
			args[1] = "fs.defaultFS=" + namenode;
			Execute(args, namenode);
		}

		private void Execute(string[] args, string namenode)
		{
			FsShell shell = new FsShell();
			FileSystem fs = null;
			try
			{
				ToolRunner.Run(shell, args);
				fs = FileSystem.Get(NameNode.GetUri(NameNode.GetAddress(namenode)), shell.GetConf
					());
				NUnit.Framework.Assert.IsTrue("Directory does not get created", fs.IsDirectory(new 
					Path("/data")));
				fs.Delete(new Path("/data"), true);
			}
			catch (Exception e)
			{
				System.Console.Error.WriteLine(e.Message);
				Sharpen.Runtime.PrintStackTrace(e);
			}
			finally
			{
				if (fs != null)
				{
					try
					{
						fs.Close();
					}
					catch (IOException)
					{
					}
				}
			}
		}
	}
}
