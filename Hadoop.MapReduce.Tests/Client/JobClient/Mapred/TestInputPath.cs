using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestInputPath : TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestInputPath()
		{
			JobConf jobConf = new JobConf();
			Path workingDir = jobConf.GetWorkingDirectory();
			Path path = new Path(workingDir, "xx{y" + StringUtils.CommaStr + "z}");
			FileInputFormat.SetInputPaths(jobConf, path);
			Path[] paths = FileInputFormat.GetInputPaths(jobConf);
			NUnit.Framework.Assert.AreEqual(1, paths.Length);
			NUnit.Framework.Assert.AreEqual(path.ToString(), paths[0].ToString());
			StringBuilder pathStr = new StringBuilder();
			pathStr.Append(StringUtils.EscapeChar);
			pathStr.Append(StringUtils.EscapeChar);
			pathStr.Append(StringUtils.Comma);
			pathStr.Append(StringUtils.Comma);
			pathStr.Append('a');
			path = new Path(workingDir, pathStr.ToString());
			FileInputFormat.SetInputPaths(jobConf, path);
			paths = FileInputFormat.GetInputPaths(jobConf);
			NUnit.Framework.Assert.AreEqual(1, paths.Length);
			NUnit.Framework.Assert.AreEqual(path.ToString(), paths[0].ToString());
			pathStr.Length = 0;
			pathStr.Append(StringUtils.EscapeChar);
			pathStr.Append("xx");
			pathStr.Append(StringUtils.EscapeChar);
			path = new Path(workingDir, pathStr.ToString());
			Path path1 = new Path(workingDir, "yy" + StringUtils.CommaStr + "zz");
			FileInputFormat.SetInputPaths(jobConf, path);
			FileInputFormat.AddInputPath(jobConf, path1);
			paths = FileInputFormat.GetInputPaths(jobConf);
			NUnit.Framework.Assert.AreEqual(2, paths.Length);
			NUnit.Framework.Assert.AreEqual(path.ToString(), paths[0].ToString());
			NUnit.Framework.Assert.AreEqual(path1.ToString(), paths[1].ToString());
			FileInputFormat.SetInputPaths(jobConf, path, path1);
			paths = FileInputFormat.GetInputPaths(jobConf);
			NUnit.Framework.Assert.AreEqual(2, paths.Length);
			NUnit.Framework.Assert.AreEqual(path.ToString(), paths[0].ToString());
			NUnit.Framework.Assert.AreEqual(path1.ToString(), paths[1].ToString());
			Path[] input = new Path[] { path, path1 };
			FileInputFormat.SetInputPaths(jobConf, input);
			paths = FileInputFormat.GetInputPaths(jobConf);
			NUnit.Framework.Assert.AreEqual(2, paths.Length);
			NUnit.Framework.Assert.AreEqual(path.ToString(), paths[0].ToString());
			NUnit.Framework.Assert.AreEqual(path1.ToString(), paths[1].ToString());
			pathStr.Length = 0;
			string str1 = "{a{b,c},de}";
			string str2 = "xyz";
			string str3 = "x{y,z}";
			pathStr.Append(str1);
			pathStr.Append(StringUtils.Comma);
			pathStr.Append(str2);
			pathStr.Append(StringUtils.Comma);
			pathStr.Append(str3);
			FileInputFormat.SetInputPaths(jobConf, pathStr.ToString());
			paths = FileInputFormat.GetInputPaths(jobConf);
			NUnit.Framework.Assert.AreEqual(3, paths.Length);
			NUnit.Framework.Assert.AreEqual(new Path(workingDir, str1).ToString(), paths[0].ToString
				());
			NUnit.Framework.Assert.AreEqual(new Path(workingDir, str2).ToString(), paths[1].ToString
				());
			NUnit.Framework.Assert.AreEqual(new Path(workingDir, str3).ToString(), paths[2].ToString
				());
			pathStr.Length = 0;
			string str4 = "abc";
			string str5 = "pq{r,s}";
			pathStr.Append(str4);
			pathStr.Append(StringUtils.Comma);
			pathStr.Append(str5);
			FileInputFormat.AddInputPaths(jobConf, pathStr.ToString());
			paths = FileInputFormat.GetInputPaths(jobConf);
			NUnit.Framework.Assert.AreEqual(5, paths.Length);
			NUnit.Framework.Assert.AreEqual(new Path(workingDir, str1).ToString(), paths[0].ToString
				());
			NUnit.Framework.Assert.AreEqual(new Path(workingDir, str2).ToString(), paths[1].ToString
				());
			NUnit.Framework.Assert.AreEqual(new Path(workingDir, str3).ToString(), paths[2].ToString
				());
			NUnit.Framework.Assert.AreEqual(new Path(workingDir, str4).ToString(), paths[3].ToString
				());
			NUnit.Framework.Assert.AreEqual(new Path(workingDir, str5).ToString(), paths[4].ToString
				());
		}
	}
}
