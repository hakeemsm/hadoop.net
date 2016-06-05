using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	public class TestFind
	{
		private static FileSystem mockFs;

		private static Configuration conf;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			mockFs = MockFileSystem.Setup();
			conf = mockFs.GetConf();
		}

		// check follow link option is recognized
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsFollowLink()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			string args = "-L path";
			find.ProcessOptions(GetArgs(args));
			Assert.True(find.GetOptions().IsFollowLink());
			NUnit.Framework.Assert.IsFalse(find.GetOptions().IsFollowArgLink());
		}

		// check follow arg link option is recognized
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsFollowArgLink()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			string args = "-H path";
			find.ProcessOptions(GetArgs(args));
			NUnit.Framework.Assert.IsFalse(find.GetOptions().IsFollowLink());
			Assert.True(find.GetOptions().IsFollowArgLink());
		}

		// check follow arg link option is recognized
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsFollowLinkFollowArgLink()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			string args = "-L -H path";
			find.ProcessOptions(GetArgs(args));
			Assert.True(find.GetOptions().IsFollowLink());
			// follow link option takes precedence over follow arg link
			NUnit.Framework.Assert.IsFalse(find.GetOptions().IsFollowArgLink());
		}

		// check options and expressions are stripped from args leaving paths
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsExpression()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string paths = "path1 path2 path3";
			string args = "-L -H " + paths + " -print -name test";
			List<string> argsList = GetArgs(args);
			find.ProcessOptions(argsList);
			List<string> pathList = GetArgs(paths);
			Assert.Equal(pathList, argsList);
		}

		// check print is used as the default expression
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsNoExpression()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string args = "path";
			string expected = "Print(;)";
			find.ProcessOptions(GetArgs(args));
			Expression expression = find.GetRootExpression();
			Assert.Equal(expected, expression.ToString());
		}

		// check unknown options are rejected
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsUnknown()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string args = "path -unknown";
			try
			{
				find.ProcessOptions(GetArgs(args));
				NUnit.Framework.Assert.Fail("Unknown expression not caught");
			}
			catch (IOException)
			{
			}
		}

		// check unknown options are rejected when mixed with known options
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsKnownUnknown()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string args = "path -print -unknown -print";
			try
			{
				find.ProcessOptions(GetArgs(args));
				NUnit.Framework.Assert.Fail("Unknown expression not caught");
			}
			catch (IOException)
			{
			}
		}

		// check no path defaults to current working directory
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsNoPath()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string args = "-print";
			List<string> argsList = GetArgs(args);
			find.ProcessOptions(argsList);
			Assert.Equal(Sharpen.Collections.SingletonList(Path.CurDir), argsList
				);
		}

		// check -name is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsName()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string args = "path -name namemask";
			string expected = "And(;Name(namemask;),Print(;))";
			find.ProcessOptions(GetArgs(args));
			Expression expression = find.GetRootExpression();
			Assert.Equal(expected, expression.ToString());
		}

		// check -iname is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsIname()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string args = "path -iname namemask";
			string expected = "And(;Iname-Name(namemask;),Print(;))";
			find.ProcessOptions(GetArgs(args));
			Expression expression = find.GetRootExpression();
			Assert.Equal(expected, expression.ToString());
		}

		// check -print is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsPrint()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string args = "path -print";
			string expected = "Print(;)";
			find.ProcessOptions(GetArgs(args));
			Expression expression = find.GetRootExpression();
			Assert.Equal(expected, expression.ToString());
		}

		// check -print0 is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsPrint0()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string args = "path -print0";
			string expected = "Print0-Print(;)";
			find.ProcessOptions(GetArgs(args));
			Expression expression = find.GetRootExpression();
			Assert.Equal(expected, expression.ToString());
		}

		// check an implicit and is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsNoop()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string args = "path -name one -name two -print";
			string expected = "And(;And(;Name(one;),Name(two;)),Print(;))";
			find.ProcessOptions(GetArgs(args));
			Expression expression = find.GetRootExpression();
			Assert.Equal(expected, expression.ToString());
		}

		// check -a is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsA()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string args = "path -name one -a -name two -a -print";
			string expected = "And(;And(;Name(one;),Name(two;)),Print(;))";
			find.ProcessOptions(GetArgs(args));
			Expression expression = find.GetRootExpression();
			Assert.Equal(expected, expression.ToString());
		}

		// check -and is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessOptionsAnd()
		{
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			string args = "path -name one -and -name two -and -print";
			string expected = "And(;And(;Name(one;),Name(two;)),Print(;))";
			find.ProcessOptions(GetArgs(args));
			Expression expression = find.GetRootExpression();
			Assert.Equal(expected, expression.ToString());
		}

		// check expressions are called in the correct order
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessArguments()
		{
			List<PathData> items = CreateDirectories();
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetOut(@out);
			TextWriter err = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetErr(err);
			Expression expr = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(expr.Apply((PathData)Matchers.Any(), Matchers.AnyInt()))
				.ThenReturn(Result.Pass);
			TestFind.FileStatusChecker fsCheck = Org.Mockito.Mockito.Mock<TestFind.FileStatusChecker
				>();
			Expression test = new TestFind.TestExpression(this, expr, fsCheck);
			find.SetRootExpression(test);
			find.ProcessArguments(items);
			InOrder inOrder = Org.Mockito.Mockito.InOrder(expr);
			inOrder.Verify(expr).SetOptions(find.GetOptions());
			inOrder.Verify(expr).Prepare();
			inOrder.Verify(expr).Apply(item1, 0);
			inOrder.Verify(expr).Apply(item1a, 1);
			inOrder.Verify(expr).Apply(item1aa, 2);
			inOrder.Verify(expr).Apply(item1b, 1);
			inOrder.Verify(expr).Apply(item2, 0);
			inOrder.Verify(expr).Apply(item3, 0);
			inOrder.Verify(expr).Apply(item4, 0);
			inOrder.Verify(expr).Apply(item5, 0);
			inOrder.Verify(expr).Apply(item5a, 1);
			inOrder.Verify(expr).Apply(item5b, 1);
			inOrder.Verify(expr).Apply(item5c, 1);
			inOrder.Verify(expr).Apply(item5ca, 2);
			inOrder.Verify(expr).Apply(item5d, 1);
			inOrder.Verify(expr).Apply(item5e, 1);
			inOrder.Verify(expr).Finish();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
			InOrder inOrderFsCheck = Org.Mockito.Mockito.InOrder(fsCheck);
			inOrderFsCheck.Verify(fsCheck).Check(item1.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1aa.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item2.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item3.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item4.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5c.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5ca.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5d.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5e.stat);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(fsCheck);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(err);
		}

		// check that directories are descended correctly when -depth is specified
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessArgumentsDepthFirst()
		{
			List<PathData> items = CreateDirectories();
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.GetOptions().SetDepthFirst(true);
			find.SetConf(conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetOut(@out);
			TextWriter err = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetErr(err);
			Expression expr = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(expr.Apply((PathData)Matchers.Any(), Matchers.AnyInt()))
				.ThenReturn(Result.Pass);
			TestFind.FileStatusChecker fsCheck = Org.Mockito.Mockito.Mock<TestFind.FileStatusChecker
				>();
			Expression test = new TestFind.TestExpression(this, expr, fsCheck);
			find.SetRootExpression(test);
			find.ProcessArguments(items);
			InOrder inOrder = Org.Mockito.Mockito.InOrder(expr);
			inOrder.Verify(expr).SetOptions(find.GetOptions());
			inOrder.Verify(expr).Prepare();
			inOrder.Verify(expr).Apply(item1aa, 2);
			inOrder.Verify(expr).Apply(item1a, 1);
			inOrder.Verify(expr).Apply(item1b, 1);
			inOrder.Verify(expr).Apply(item1, 0);
			inOrder.Verify(expr).Apply(item2, 0);
			inOrder.Verify(expr).Apply(item3, 0);
			inOrder.Verify(expr).Apply(item4, 0);
			inOrder.Verify(expr).Apply(item5a, 1);
			inOrder.Verify(expr).Apply(item5b, 1);
			inOrder.Verify(expr).Apply(item5ca, 2);
			inOrder.Verify(expr).Apply(item5c, 1);
			inOrder.Verify(expr).Apply(item5d, 1);
			inOrder.Verify(expr).Apply(item5e, 1);
			inOrder.Verify(expr).Apply(item5, 0);
			inOrder.Verify(expr).Finish();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
			InOrder inOrderFsCheck = Org.Mockito.Mockito.InOrder(fsCheck);
			inOrderFsCheck.Verify(fsCheck).Check(item1aa.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item2.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item3.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item4.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5ca.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5c.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5d.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5e.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5.stat);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(fsCheck);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(err);
		}

		// check symlinks given as path arguments are processed correctly with the
		// follow arg option set
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessArgumentsOptionFollowArg()
		{
			List<PathData> items = CreateDirectories();
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.GetOptions().SetFollowArgLink(true);
			find.SetConf(conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetOut(@out);
			TextWriter err = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetErr(err);
			Expression expr = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(expr.Apply((PathData)Matchers.Any(), Matchers.AnyInt()))
				.ThenReturn(Result.Pass);
			TestFind.FileStatusChecker fsCheck = Org.Mockito.Mockito.Mock<TestFind.FileStatusChecker
				>();
			Expression test = new TestFind.TestExpression(this, expr, fsCheck);
			find.SetRootExpression(test);
			find.ProcessArguments(items);
			InOrder inOrder = Org.Mockito.Mockito.InOrder(expr);
			inOrder.Verify(expr).SetOptions(find.GetOptions());
			inOrder.Verify(expr).Prepare();
			inOrder.Verify(expr).Apply(item1, 0);
			inOrder.Verify(expr).Apply(item1a, 1);
			inOrder.Verify(expr).Apply(item1aa, 2);
			inOrder.Verify(expr).Apply(item1b, 1);
			inOrder.Verify(expr).Apply(item2, 0);
			inOrder.Verify(expr).Apply(item3, 0);
			inOrder.Verify(expr).Apply(item4, 0);
			inOrder.Verify(expr).Apply(item5, 0);
			inOrder.Verify(expr).Apply(item5a, 1);
			inOrder.Verify(expr).Apply(item5b, 1);
			inOrder.Verify(expr).Apply(item5c, 1);
			inOrder.Verify(expr).Apply(item5ca, 2);
			inOrder.Verify(expr).Apply(item5d, 1);
			inOrder.Verify(expr).Apply(item5e, 1);
			inOrder.Verify(expr).Finish();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
			InOrder inOrderFsCheck = Org.Mockito.Mockito.InOrder(fsCheck);
			inOrderFsCheck.Verify(fsCheck).Check(item1.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1aa.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item2.stat);
			inOrderFsCheck.Verify(fsCheck, Org.Mockito.Mockito.Times(2)).Check(item3.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5c.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5ca.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5d.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5e.stat);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(fsCheck);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(err);
		}

		// check symlinks given as path arguments are processed correctly with the
		// follow option
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessArgumentsOptionFollow()
		{
			List<PathData> items = CreateDirectories();
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.GetOptions().SetFollowLink(true);
			find.SetConf(conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetOut(@out);
			TextWriter err = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetErr(err);
			Expression expr = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(expr.Apply((PathData)Matchers.Any(), Matchers.AnyInt()))
				.ThenReturn(Result.Pass);
			TestFind.FileStatusChecker fsCheck = Org.Mockito.Mockito.Mock<TestFind.FileStatusChecker
				>();
			Expression test = new TestFind.TestExpression(this, expr, fsCheck);
			find.SetRootExpression(test);
			find.ProcessArguments(items);
			InOrder inOrder = Org.Mockito.Mockito.InOrder(expr);
			inOrder.Verify(expr).SetOptions(find.GetOptions());
			inOrder.Verify(expr).Prepare();
			inOrder.Verify(expr).Apply(item1, 0);
			inOrder.Verify(expr).Apply(item1a, 1);
			inOrder.Verify(expr).Apply(item1aa, 2);
			inOrder.Verify(expr).Apply(item1b, 1);
			inOrder.Verify(expr).Apply(item2, 0);
			inOrder.Verify(expr).Apply(item3, 0);
			inOrder.Verify(expr).Apply(item4, 0);
			inOrder.Verify(expr).Apply(item5, 0);
			inOrder.Verify(expr).Apply(item5a, 1);
			inOrder.Verify(expr).Apply(item5b, 1);
			// triggers infinite loop message
			inOrder.Verify(expr).Apply(item5c, 1);
			inOrder.Verify(expr).Apply(item5ca, 2);
			inOrder.Verify(expr).Apply(item5d, 1);
			inOrder.Verify(expr).Apply(item5ca, 2);
			// following item5d symlink
			inOrder.Verify(expr).Apply(item5e, 1);
			inOrder.Verify(expr).Finish();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
			InOrder inOrderFsCheck = Org.Mockito.Mockito.InOrder(fsCheck);
			inOrderFsCheck.Verify(fsCheck).Check(item1.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1aa.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item2.stat);
			inOrderFsCheck.Verify(fsCheck, Org.Mockito.Mockito.Times(2)).Check(item3.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5c.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5ca.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5c.stat);
			inOrderFsCheck.Verify(fsCheck, Org.Mockito.Mockito.Times(2)).Check(item5ca.stat);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(fsCheck);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
			Org.Mockito.Mockito.Verify(err).WriteLine("Infinite loop ignored: " + item5b.ToString
				() + " -> " + item5.ToString());
			Org.Mockito.Mockito.VerifyNoMoreInteractions(err);
		}

		// check minimum depth is handledfollowLink
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessArgumentsMinDepth()
		{
			List<PathData> items = CreateDirectories();
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.GetOptions().SetMinDepth(1);
			find.SetConf(conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetOut(@out);
			TextWriter err = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetErr(err);
			Expression expr = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(expr.Apply((PathData)Matchers.Any(), Matchers.AnyInt()))
				.ThenReturn(Result.Pass);
			TestFind.FileStatusChecker fsCheck = Org.Mockito.Mockito.Mock<TestFind.FileStatusChecker
				>();
			Expression test = new TestFind.TestExpression(this, expr, fsCheck);
			find.SetRootExpression(test);
			find.ProcessArguments(items);
			InOrder inOrder = Org.Mockito.Mockito.InOrder(expr);
			inOrder.Verify(expr).SetOptions(find.GetOptions());
			inOrder.Verify(expr).Prepare();
			inOrder.Verify(expr).Apply(item1a, 1);
			inOrder.Verify(expr).Apply(item1aa, 2);
			inOrder.Verify(expr).Apply(item1b, 1);
			inOrder.Verify(expr).Apply(item5a, 1);
			inOrder.Verify(expr).Apply(item5b, 1);
			inOrder.Verify(expr).Apply(item5c, 1);
			inOrder.Verify(expr).Apply(item5ca, 2);
			inOrder.Verify(expr).Apply(item5d, 1);
			inOrder.Verify(expr).Apply(item5e, 1);
			inOrder.Verify(expr).Finish();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
			InOrder inOrderFsCheck = Org.Mockito.Mockito.InOrder(fsCheck);
			inOrderFsCheck.Verify(fsCheck).Check(item1a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1aa.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5c.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5ca.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5d.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5e.stat);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(fsCheck);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(err);
		}

		// check maximum depth is handled
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessArgumentsMaxDepth()
		{
			List<PathData> items = CreateDirectories();
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.GetOptions().SetMaxDepth(1);
			find.SetConf(conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetOut(@out);
			TextWriter err = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetErr(err);
			Expression expr = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(expr.Apply((PathData)Matchers.Any(), Matchers.AnyInt()))
				.ThenReturn(Result.Pass);
			TestFind.FileStatusChecker fsCheck = Org.Mockito.Mockito.Mock<TestFind.FileStatusChecker
				>();
			Expression test = new TestFind.TestExpression(this, expr, fsCheck);
			find.SetRootExpression(test);
			find.ProcessArguments(items);
			InOrder inOrder = Org.Mockito.Mockito.InOrder(expr);
			inOrder.Verify(expr).SetOptions(find.GetOptions());
			inOrder.Verify(expr).Prepare();
			inOrder.Verify(expr).Apply(item1, 0);
			inOrder.Verify(expr).Apply(item1a, 1);
			inOrder.Verify(expr).Apply(item1b, 1);
			inOrder.Verify(expr).Apply(item2, 0);
			inOrder.Verify(expr).Apply(item3, 0);
			inOrder.Verify(expr).Apply(item4, 0);
			inOrder.Verify(expr).Apply(item5, 0);
			inOrder.Verify(expr).Apply(item5a, 1);
			inOrder.Verify(expr).Apply(item5b, 1);
			inOrder.Verify(expr).Apply(item5c, 1);
			inOrder.Verify(expr).Apply(item5d, 1);
			inOrder.Verify(expr).Apply(item5e, 1);
			inOrder.Verify(expr).Finish();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
			InOrder inOrderFsCheck = Org.Mockito.Mockito.InOrder(fsCheck);
			inOrderFsCheck.Verify(fsCheck).Check(item1.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item2.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item3.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item4.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5c.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5d.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5e.stat);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(fsCheck);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(err);
		}

		// check min depth is handled when -depth is specified
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessArgumentsDepthFirstMinDepth()
		{
			List<PathData> items = CreateDirectories();
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.GetOptions().SetDepthFirst(true);
			find.GetOptions().SetMinDepth(1);
			find.SetConf(conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetOut(@out);
			TextWriter err = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetErr(err);
			Expression expr = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(expr.Apply((PathData)Matchers.Any(), Matchers.AnyInt()))
				.ThenReturn(Result.Pass);
			TestFind.FileStatusChecker fsCheck = Org.Mockito.Mockito.Mock<TestFind.FileStatusChecker
				>();
			Expression test = new TestFind.TestExpression(this, expr, fsCheck);
			find.SetRootExpression(test);
			find.ProcessArguments(items);
			InOrder inOrder = Org.Mockito.Mockito.InOrder(expr);
			inOrder.Verify(expr).SetOptions(find.GetOptions());
			inOrder.Verify(expr).Prepare();
			inOrder.Verify(expr).Apply(item1aa, 2);
			inOrder.Verify(expr).Apply(item1a, 1);
			inOrder.Verify(expr).Apply(item1b, 1);
			inOrder.Verify(expr).Apply(item5a, 1);
			inOrder.Verify(expr).Apply(item5b, 1);
			inOrder.Verify(expr).Apply(item5ca, 2);
			inOrder.Verify(expr).Apply(item5c, 1);
			inOrder.Verify(expr).Apply(item5d, 1);
			inOrder.Verify(expr).Apply(item5e, 1);
			inOrder.Verify(expr).Finish();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
			InOrder inOrderFsCheck = Org.Mockito.Mockito.InOrder(fsCheck);
			inOrderFsCheck.Verify(fsCheck).Check(item1aa.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5ca.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5c.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5d.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5e.stat);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(fsCheck);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(err);
		}

		// check max depth is handled when -depth is specified
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessArgumentsDepthFirstMaxDepth()
		{
			List<PathData> items = CreateDirectories();
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.GetOptions().SetDepthFirst(true);
			find.GetOptions().SetMaxDepth(1);
			find.SetConf(conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetOut(@out);
			TextWriter err = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetErr(err);
			Expression expr = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(expr.Apply((PathData)Matchers.Any(), Matchers.AnyInt()))
				.ThenReturn(Result.Pass);
			TestFind.FileStatusChecker fsCheck = Org.Mockito.Mockito.Mock<TestFind.FileStatusChecker
				>();
			Expression test = new TestFind.TestExpression(this, expr, fsCheck);
			find.SetRootExpression(test);
			find.ProcessArguments(items);
			InOrder inOrder = Org.Mockito.Mockito.InOrder(expr);
			inOrder.Verify(expr).SetOptions(find.GetOptions());
			inOrder.Verify(expr).Prepare();
			inOrder.Verify(expr).Apply(item1a, 1);
			inOrder.Verify(expr).Apply(item1b, 1);
			inOrder.Verify(expr).Apply(item1, 0);
			inOrder.Verify(expr).Apply(item2, 0);
			inOrder.Verify(expr).Apply(item3, 0);
			inOrder.Verify(expr).Apply(item4, 0);
			inOrder.Verify(expr).Apply(item5a, 1);
			inOrder.Verify(expr).Apply(item5b, 1);
			inOrder.Verify(expr).Apply(item5c, 1);
			inOrder.Verify(expr).Apply(item5d, 1);
			inOrder.Verify(expr).Apply(item5e, 1);
			inOrder.Verify(expr).Apply(item5, 0);
			inOrder.Verify(expr).Finish();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
			InOrder inOrderFsCheck = Org.Mockito.Mockito.InOrder(fsCheck);
			inOrderFsCheck.Verify(fsCheck).Check(item1a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item2.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item3.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item4.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5c.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5d.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5e.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5.stat);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(fsCheck);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(err);
		}

		// check expressions are called in the correct order
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessArgumentsNoDescend()
		{
			List<PathData> items = CreateDirectories();
			Org.Apache.Hadoop.FS.Shell.Find.Find find = new Org.Apache.Hadoop.FS.Shell.Find.Find
				();
			find.SetConf(conf);
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetOut(@out);
			TextWriter err = Org.Mockito.Mockito.Mock<TextWriter>();
			find.GetOptions().SetErr(err);
			Expression expr = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(expr.Apply((PathData)Matchers.Any(), Matchers.AnyInt()))
				.ThenReturn(Result.Pass);
			Org.Mockito.Mockito.When(expr.Apply(Matchers.Eq(item1a), Matchers.AnyInt())).ThenReturn
				(Result.Stop);
			TestFind.FileStatusChecker fsCheck = Org.Mockito.Mockito.Mock<TestFind.FileStatusChecker
				>();
			Expression test = new TestFind.TestExpression(this, expr, fsCheck);
			find.SetRootExpression(test);
			find.ProcessArguments(items);
			InOrder inOrder = Org.Mockito.Mockito.InOrder(expr);
			inOrder.Verify(expr).SetOptions(find.GetOptions());
			inOrder.Verify(expr).Prepare();
			inOrder.Verify(expr).Apply(item1, 0);
			inOrder.Verify(expr).Apply(item1a, 1);
			inOrder.Verify(expr).Apply(item1b, 1);
			inOrder.Verify(expr).Apply(item2, 0);
			inOrder.Verify(expr).Apply(item3, 0);
			inOrder.Verify(expr).Apply(item4, 0);
			inOrder.Verify(expr).Apply(item5, 0);
			inOrder.Verify(expr).Apply(item5a, 1);
			inOrder.Verify(expr).Apply(item5b, 1);
			inOrder.Verify(expr).Apply(item5c, 1);
			inOrder.Verify(expr).Apply(item5ca, 2);
			inOrder.Verify(expr).Apply(item5d, 1);
			inOrder.Verify(expr).Apply(item5e, 1);
			inOrder.Verify(expr).Finish();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
			InOrder inOrderFsCheck = Org.Mockito.Mockito.InOrder(fsCheck);
			inOrderFsCheck.Verify(fsCheck).Check(item1.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item1b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item2.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item3.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item4.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5a.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5b.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5c.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5ca.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5d.stat);
			inOrderFsCheck.Verify(fsCheck).Check(item5e.stat);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(fsCheck);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(err);
		}

		private interface FileStatusChecker
		{
			void Check(FileStatus fileStatus);
		}

		private class TestExpression : BaseExpression, Expression
		{
			private Expression expr;

			private TestFind.FileStatusChecker checker;

			public TestExpression(TestFind _enclosing, Expression expr, TestFind.FileStatusChecker
				 checker)
			{
				this._enclosing = _enclosing;
				this.expr = expr;
				this.checker = checker;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Result Apply(PathData item, int depth)
			{
				FileStatus fileStatus = this.GetFileStatus(item, depth);
				this.checker.Check(fileStatus);
				return this.expr.Apply(item, depth);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetOptions(FindOptions options)
			{
				base.SetOptions(options);
				this.expr.SetOptions(options);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Prepare()
			{
				this.expr.Prepare();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Finish()
			{
				this.expr.Finish();
			}

			private readonly TestFind _enclosing;
		}

		private PathData item1 = null;

		private PathData item1a = null;

		private PathData item1aa = null;

		private PathData item1b = null;

		private PathData item2 = null;

		private PathData item3 = null;

		private PathData item4 = null;

		private PathData item5 = null;

		private PathData item5a = null;

		private PathData item5b = null;

		private PathData item5c = null;

		private PathData item5ca = null;

		private PathData item5d = null;

		private PathData item5e = null;

		// creates a directory structure for traversal
		// item1 (directory)
		// \- item1a (directory)
		//    \- item1aa (file)
		// \- item1b (file)
		// item2 (directory)
		// item3 (file)
		// item4 (link) -> item3
		// item5 (directory)
		// \- item5a (link) -> item1b
		// \- item5b (link) -> item5 (infinite loop)
		// \- item5c (directory)
		//    \- item5ca (file)
		// \- item5d (link) -> item5c
		// \- item5e (link) -> item5c/item5ca
		/// <exception cref="System.IO.IOException"/>
		private List<PathData> CreateDirectories()
		{
			item1 = CreatePathData("item1");
			item1a = CreatePathData("item1/item1a");
			item1aa = CreatePathData("item1/item1a/item1aa");
			item1b = CreatePathData("item1/item1b");
			item2 = CreatePathData("item2");
			item3 = CreatePathData("item3");
			item4 = CreatePathData("item4");
			item5 = CreatePathData("item5");
			item5a = CreatePathData("item5/item5a");
			item5b = CreatePathData("item5/item5b");
			item5c = CreatePathData("item5/item5c");
			item5ca = CreatePathData("item5/item5c/item5ca");
			item5d = CreatePathData("item5/item5d");
			item5e = CreatePathData("item5/item5e");
			List<PathData> args = new List<PathData>();
			Org.Mockito.Mockito.When(item1.stat.IsDirectory()).ThenReturn(true);
			Org.Mockito.Mockito.When(item1a.stat.IsDirectory()).ThenReturn(true);
			Org.Mockito.Mockito.When(item1aa.stat.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(item1b.stat.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(item2.stat.IsDirectory()).ThenReturn(true);
			Org.Mockito.Mockito.When(item3.stat.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(item4.stat.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(item5.stat.IsDirectory()).ThenReturn(true);
			Org.Mockito.Mockito.When(item5a.stat.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(item5b.stat.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(item5c.stat.IsDirectory()).ThenReturn(true);
			Org.Mockito.Mockito.When(item5ca.stat.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(item5d.stat.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(item5e.stat.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(mockFs.ListStatus(Matchers.Eq(item1.path))).ThenReturn(new 
				FileStatus[] { item1a.stat, item1b.stat });
			Org.Mockito.Mockito.When(mockFs.ListStatus(Matchers.Eq(item1a.path))).ThenReturn(
				new FileStatus[] { item1aa.stat });
			Org.Mockito.Mockito.When(mockFs.ListStatus(Matchers.Eq(item2.path))).ThenReturn(new 
				FileStatus[0]);
			Org.Mockito.Mockito.When(mockFs.ListStatus(Matchers.Eq(item5.path))).ThenReturn(new 
				FileStatus[] { item5a.stat, item5b.stat, item5c.stat, item5d.stat, item5e.stat }
				);
			Org.Mockito.Mockito.When(mockFs.ListStatus(Matchers.Eq(item5c.path))).ThenReturn(
				new FileStatus[] { item5ca.stat });
			Org.Mockito.Mockito.When(item1.stat.IsSymlink()).ThenReturn(false);
			Org.Mockito.Mockito.When(item1a.stat.IsSymlink()).ThenReturn(false);
			Org.Mockito.Mockito.When(item1aa.stat.IsSymlink()).ThenReturn(false);
			Org.Mockito.Mockito.When(item1b.stat.IsSymlink()).ThenReturn(false);
			Org.Mockito.Mockito.When(item2.stat.IsSymlink()).ThenReturn(false);
			Org.Mockito.Mockito.When(item3.stat.IsSymlink()).ThenReturn(false);
			Org.Mockito.Mockito.When(item4.stat.IsSymlink()).ThenReturn(true);
			Org.Mockito.Mockito.When(item5.stat.IsSymlink()).ThenReturn(false);
			Org.Mockito.Mockito.When(item5a.stat.IsSymlink()).ThenReturn(true);
			Org.Mockito.Mockito.When(item5b.stat.IsSymlink()).ThenReturn(true);
			Org.Mockito.Mockito.When(item5d.stat.IsSymlink()).ThenReturn(true);
			Org.Mockito.Mockito.When(item5e.stat.IsSymlink()).ThenReturn(true);
			Org.Mockito.Mockito.When(item4.stat.GetSymlink()).ThenReturn(item3.path);
			Org.Mockito.Mockito.When(item5a.stat.GetSymlink()).ThenReturn(item1b.path);
			Org.Mockito.Mockito.When(item5b.stat.GetSymlink()).ThenReturn(item5.path);
			Org.Mockito.Mockito.When(item5d.stat.GetSymlink()).ThenReturn(item5c.path);
			Org.Mockito.Mockito.When(item5e.stat.GetSymlink()).ThenReturn(item5ca.path);
			args.AddItem(item1);
			args.AddItem(item2);
			args.AddItem(item3);
			args.AddItem(item4);
			args.AddItem(item5);
			return args;
		}

		/// <exception cref="System.IO.IOException"/>
		private PathData CreatePathData(string name)
		{
			Path path = new Path(name);
			FileStatus fstat = Org.Mockito.Mockito.Mock<FileStatus>();
			Org.Mockito.Mockito.When(fstat.GetPath()).ThenReturn(path);
			Org.Mockito.Mockito.When(fstat.ToString()).ThenReturn("fileStatus:" + name);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Matchers.Eq(path))).ThenReturn(fstat
				);
			PathData item = new PathData(path.ToString(), conf);
			return item;
		}

		private List<string> GetArgs(string cmd)
		{
			return new List<string>(Arrays.AsList(cmd.Split(" ")));
		}
	}
}
