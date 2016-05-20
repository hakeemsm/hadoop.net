using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	public class TestFind
	{
		private static org.apache.hadoop.fs.FileSystem mockFs;

		private static org.apache.hadoop.conf.Configuration conf;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			mockFs = org.apache.hadoop.fs.shell.find.MockFileSystem.setup();
			conf = mockFs.getConf();
		}

		// check follow link option is recognized
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsFollowLink()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			string args = "-L path";
			find.processOptions(getArgs(args));
			NUnit.Framework.Assert.IsTrue(find.getOptions().isFollowLink());
			NUnit.Framework.Assert.IsFalse(find.getOptions().isFollowArgLink());
		}

		// check follow arg link option is recognized
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsFollowArgLink()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			string args = "-H path";
			find.processOptions(getArgs(args));
			NUnit.Framework.Assert.IsFalse(find.getOptions().isFollowLink());
			NUnit.Framework.Assert.IsTrue(find.getOptions().isFollowArgLink());
		}

		// check follow arg link option is recognized
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsFollowLinkFollowArgLink()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			string args = "-L -H path";
			find.processOptions(getArgs(args));
			NUnit.Framework.Assert.IsTrue(find.getOptions().isFollowLink());
			// follow link option takes precedence over follow arg link
			NUnit.Framework.Assert.IsFalse(find.getOptions().isFollowArgLink());
		}

		// check options and expressions are stripped from args leaving paths
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsExpression()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string paths = "path1 path2 path3";
			string args = "-L -H " + paths + " -print -name test";
			System.Collections.Generic.LinkedList<string> argsList = getArgs(args);
			find.processOptions(argsList);
			System.Collections.Generic.LinkedList<string> pathList = getArgs(paths);
			NUnit.Framework.Assert.AreEqual(pathList, argsList);
		}

		// check print is used as the default expression
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsNoExpression()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string args = "path";
			string expected = "Print(;)";
			find.processOptions(getArgs(args));
			org.apache.hadoop.fs.shell.find.Expression expression = find.getRootExpression();
			NUnit.Framework.Assert.AreEqual(expected, expression.ToString());
		}

		// check unknown options are rejected
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsUnknown()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string args = "path -unknown";
			try
			{
				find.processOptions(getArgs(args));
				NUnit.Framework.Assert.Fail("Unknown expression not caught");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// check unknown options are rejected when mixed with known options
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsKnownUnknown()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string args = "path -print -unknown -print";
			try
			{
				find.processOptions(getArgs(args));
				NUnit.Framework.Assert.Fail("Unknown expression not caught");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// check no path defaults to current working directory
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsNoPath()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string args = "-print";
			System.Collections.Generic.LinkedList<string> argsList = getArgs(args);
			find.processOptions(argsList);
			NUnit.Framework.Assert.AreEqual(java.util.Collections.singletonList(org.apache.hadoop.fs.Path
				.CUR_DIR), argsList);
		}

		// check -name is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsName()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string args = "path -name namemask";
			string expected = "And(;Name(namemask;),Print(;))";
			find.processOptions(getArgs(args));
			org.apache.hadoop.fs.shell.find.Expression expression = find.getRootExpression();
			NUnit.Framework.Assert.AreEqual(expected, expression.ToString());
		}

		// check -iname is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsIname()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string args = "path -iname namemask";
			string expected = "And(;Iname-Name(namemask;),Print(;))";
			find.processOptions(getArgs(args));
			org.apache.hadoop.fs.shell.find.Expression expression = find.getRootExpression();
			NUnit.Framework.Assert.AreEqual(expected, expression.ToString());
		}

		// check -print is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsPrint()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string args = "path -print";
			string expected = "Print(;)";
			find.processOptions(getArgs(args));
			org.apache.hadoop.fs.shell.find.Expression expression = find.getRootExpression();
			NUnit.Framework.Assert.AreEqual(expected, expression.ToString());
		}

		// check -print0 is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsPrint0()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string args = "path -print0";
			string expected = "Print0-Print(;)";
			find.processOptions(getArgs(args));
			org.apache.hadoop.fs.shell.find.Expression expression = find.getRootExpression();
			NUnit.Framework.Assert.AreEqual(expected, expression.ToString());
		}

		// check an implicit and is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsNoop()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string args = "path -name one -name two -print";
			string expected = "And(;And(;Name(one;),Name(two;)),Print(;))";
			find.processOptions(getArgs(args));
			org.apache.hadoop.fs.shell.find.Expression expression = find.getRootExpression();
			NUnit.Framework.Assert.AreEqual(expected, expression.ToString());
		}

		// check -a is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsA()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string args = "path -name one -a -name two -a -print";
			string expected = "And(;And(;Name(one;),Name(two;)),Print(;))";
			find.processOptions(getArgs(args));
			org.apache.hadoop.fs.shell.find.Expression expression = find.getRootExpression();
			NUnit.Framework.Assert.AreEqual(expected, expression.ToString());
		}

		// check -and is handled correctly
		/// <exception cref="System.IO.IOException"/>
		public virtual void processOptionsAnd()
		{
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			string args = "path -name one -and -name two -and -print";
			string expected = "And(;And(;Name(one;),Name(two;)),Print(;))";
			find.processOptions(getArgs(args));
			org.apache.hadoop.fs.shell.find.Expression expression = find.getRootExpression();
			NUnit.Framework.Assert.AreEqual(expected, expression.ToString());
		}

		// check expressions are called in the correct order
		/// <exception cref="System.IO.IOException"/>
		public virtual void processArguments()
		{
			System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData> items = 
				createDirectories();
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setOut(@out);
			System.IO.TextWriter err = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setErr(err);
			org.apache.hadoop.fs.shell.find.Expression expr = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(expr.apply((org.apache.hadoop.fs.shell.PathData)org.mockito.Matchers.any
				(), org.mockito.Matchers.anyInt())).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker fsCheck = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker>();
			org.apache.hadoop.fs.shell.find.Expression test = new org.apache.hadoop.fs.shell.find.TestFind.TestExpression
				(this, expr, fsCheck);
			find.setRootExpression(test);
			find.processArguments(items);
			org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(expr);
			inOrder.verify(expr).setOptions(find.getOptions());
			inOrder.verify(expr).prepare();
			inOrder.verify(expr).apply(item1, 0);
			inOrder.verify(expr).apply(item1a, 1);
			inOrder.verify(expr).apply(item1aa, 2);
			inOrder.verify(expr).apply(item1b, 1);
			inOrder.verify(expr).apply(item2, 0);
			inOrder.verify(expr).apply(item3, 0);
			inOrder.verify(expr).apply(item4, 0);
			inOrder.verify(expr).apply(item5, 0);
			inOrder.verify(expr).apply(item5a, 1);
			inOrder.verify(expr).apply(item5b, 1);
			inOrder.verify(expr).apply(item5c, 1);
			inOrder.verify(expr).apply(item5ca, 2);
			inOrder.verify(expr).apply(item5d, 1);
			inOrder.verify(expr).apply(item5e, 1);
			inOrder.verify(expr).finish();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
			org.mockito.InOrder inOrderFsCheck = org.mockito.Mockito.inOrder(fsCheck);
			inOrderFsCheck.verify(fsCheck).check(item1.stat);
			inOrderFsCheck.verify(fsCheck).check(item1a.stat);
			inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
			inOrderFsCheck.verify(fsCheck).check(item1b.stat);
			inOrderFsCheck.verify(fsCheck).check(item2.stat);
			inOrderFsCheck.verify(fsCheck).check(item3.stat);
			inOrderFsCheck.verify(fsCheck).check(item4.stat);
			inOrderFsCheck.verify(fsCheck).check(item5.stat);
			inOrderFsCheck.verify(fsCheck).check(item5a.stat);
			inOrderFsCheck.verify(fsCheck).check(item5b.stat);
			inOrderFsCheck.verify(fsCheck).check(item5c.stat);
			inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
			inOrderFsCheck.verify(fsCheck).check(item5d.stat);
			inOrderFsCheck.verify(fsCheck).check(item5e.stat);
			org.mockito.Mockito.verifyNoMoreInteractions(fsCheck);
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
			org.mockito.Mockito.verifyNoMoreInteractions(err);
		}

		// check that directories are descended correctly when -depth is specified
		/// <exception cref="System.IO.IOException"/>
		public virtual void processArgumentsDepthFirst()
		{
			System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData> items = 
				createDirectories();
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.getOptions().setDepthFirst(true);
			find.setConf(conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setOut(@out);
			System.IO.TextWriter err = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setErr(err);
			org.apache.hadoop.fs.shell.find.Expression expr = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(expr.apply((org.apache.hadoop.fs.shell.PathData)org.mockito.Matchers.any
				(), org.mockito.Matchers.anyInt())).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker fsCheck = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker>();
			org.apache.hadoop.fs.shell.find.Expression test = new org.apache.hadoop.fs.shell.find.TestFind.TestExpression
				(this, expr, fsCheck);
			find.setRootExpression(test);
			find.processArguments(items);
			org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(expr);
			inOrder.verify(expr).setOptions(find.getOptions());
			inOrder.verify(expr).prepare();
			inOrder.verify(expr).apply(item1aa, 2);
			inOrder.verify(expr).apply(item1a, 1);
			inOrder.verify(expr).apply(item1b, 1);
			inOrder.verify(expr).apply(item1, 0);
			inOrder.verify(expr).apply(item2, 0);
			inOrder.verify(expr).apply(item3, 0);
			inOrder.verify(expr).apply(item4, 0);
			inOrder.verify(expr).apply(item5a, 1);
			inOrder.verify(expr).apply(item5b, 1);
			inOrder.verify(expr).apply(item5ca, 2);
			inOrder.verify(expr).apply(item5c, 1);
			inOrder.verify(expr).apply(item5d, 1);
			inOrder.verify(expr).apply(item5e, 1);
			inOrder.verify(expr).apply(item5, 0);
			inOrder.verify(expr).finish();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
			org.mockito.InOrder inOrderFsCheck = org.mockito.Mockito.inOrder(fsCheck);
			inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
			inOrderFsCheck.verify(fsCheck).check(item1a.stat);
			inOrderFsCheck.verify(fsCheck).check(item1b.stat);
			inOrderFsCheck.verify(fsCheck).check(item1.stat);
			inOrderFsCheck.verify(fsCheck).check(item2.stat);
			inOrderFsCheck.verify(fsCheck).check(item3.stat);
			inOrderFsCheck.verify(fsCheck).check(item4.stat);
			inOrderFsCheck.verify(fsCheck).check(item5a.stat);
			inOrderFsCheck.verify(fsCheck).check(item5b.stat);
			inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
			inOrderFsCheck.verify(fsCheck).check(item5c.stat);
			inOrderFsCheck.verify(fsCheck).check(item5d.stat);
			inOrderFsCheck.verify(fsCheck).check(item5e.stat);
			inOrderFsCheck.verify(fsCheck).check(item5.stat);
			org.mockito.Mockito.verifyNoMoreInteractions(fsCheck);
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
			org.mockito.Mockito.verifyNoMoreInteractions(err);
		}

		// check symlinks given as path arguments are processed correctly with the
		// follow arg option set
		/// <exception cref="System.IO.IOException"/>
		public virtual void processArgumentsOptionFollowArg()
		{
			System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData> items = 
				createDirectories();
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.getOptions().setFollowArgLink(true);
			find.setConf(conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setOut(@out);
			System.IO.TextWriter err = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setErr(err);
			org.apache.hadoop.fs.shell.find.Expression expr = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(expr.apply((org.apache.hadoop.fs.shell.PathData)org.mockito.Matchers.any
				(), org.mockito.Matchers.anyInt())).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker fsCheck = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker>();
			org.apache.hadoop.fs.shell.find.Expression test = new org.apache.hadoop.fs.shell.find.TestFind.TestExpression
				(this, expr, fsCheck);
			find.setRootExpression(test);
			find.processArguments(items);
			org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(expr);
			inOrder.verify(expr).setOptions(find.getOptions());
			inOrder.verify(expr).prepare();
			inOrder.verify(expr).apply(item1, 0);
			inOrder.verify(expr).apply(item1a, 1);
			inOrder.verify(expr).apply(item1aa, 2);
			inOrder.verify(expr).apply(item1b, 1);
			inOrder.verify(expr).apply(item2, 0);
			inOrder.verify(expr).apply(item3, 0);
			inOrder.verify(expr).apply(item4, 0);
			inOrder.verify(expr).apply(item5, 0);
			inOrder.verify(expr).apply(item5a, 1);
			inOrder.verify(expr).apply(item5b, 1);
			inOrder.verify(expr).apply(item5c, 1);
			inOrder.verify(expr).apply(item5ca, 2);
			inOrder.verify(expr).apply(item5d, 1);
			inOrder.verify(expr).apply(item5e, 1);
			inOrder.verify(expr).finish();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
			org.mockito.InOrder inOrderFsCheck = org.mockito.Mockito.inOrder(fsCheck);
			inOrderFsCheck.verify(fsCheck).check(item1.stat);
			inOrderFsCheck.verify(fsCheck).check(item1a.stat);
			inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
			inOrderFsCheck.verify(fsCheck).check(item1b.stat);
			inOrderFsCheck.verify(fsCheck).check(item2.stat);
			inOrderFsCheck.verify(fsCheck, org.mockito.Mockito.times(2)).check(item3.stat);
			inOrderFsCheck.verify(fsCheck).check(item5.stat);
			inOrderFsCheck.verify(fsCheck).check(item5a.stat);
			inOrderFsCheck.verify(fsCheck).check(item5b.stat);
			inOrderFsCheck.verify(fsCheck).check(item5c.stat);
			inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
			inOrderFsCheck.verify(fsCheck).check(item5d.stat);
			inOrderFsCheck.verify(fsCheck).check(item5e.stat);
			org.mockito.Mockito.verifyNoMoreInteractions(fsCheck);
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
			org.mockito.Mockito.verifyNoMoreInteractions(err);
		}

		// check symlinks given as path arguments are processed correctly with the
		// follow option
		/// <exception cref="System.IO.IOException"/>
		public virtual void processArgumentsOptionFollow()
		{
			System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData> items = 
				createDirectories();
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.getOptions().setFollowLink(true);
			find.setConf(conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setOut(@out);
			System.IO.TextWriter err = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setErr(err);
			org.apache.hadoop.fs.shell.find.Expression expr = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(expr.apply((org.apache.hadoop.fs.shell.PathData)org.mockito.Matchers.any
				(), org.mockito.Matchers.anyInt())).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker fsCheck = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker>();
			org.apache.hadoop.fs.shell.find.Expression test = new org.apache.hadoop.fs.shell.find.TestFind.TestExpression
				(this, expr, fsCheck);
			find.setRootExpression(test);
			find.processArguments(items);
			org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(expr);
			inOrder.verify(expr).setOptions(find.getOptions());
			inOrder.verify(expr).prepare();
			inOrder.verify(expr).apply(item1, 0);
			inOrder.verify(expr).apply(item1a, 1);
			inOrder.verify(expr).apply(item1aa, 2);
			inOrder.verify(expr).apply(item1b, 1);
			inOrder.verify(expr).apply(item2, 0);
			inOrder.verify(expr).apply(item3, 0);
			inOrder.verify(expr).apply(item4, 0);
			inOrder.verify(expr).apply(item5, 0);
			inOrder.verify(expr).apply(item5a, 1);
			inOrder.verify(expr).apply(item5b, 1);
			// triggers infinite loop message
			inOrder.verify(expr).apply(item5c, 1);
			inOrder.verify(expr).apply(item5ca, 2);
			inOrder.verify(expr).apply(item5d, 1);
			inOrder.verify(expr).apply(item5ca, 2);
			// following item5d symlink
			inOrder.verify(expr).apply(item5e, 1);
			inOrder.verify(expr).finish();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
			org.mockito.InOrder inOrderFsCheck = org.mockito.Mockito.inOrder(fsCheck);
			inOrderFsCheck.verify(fsCheck).check(item1.stat);
			inOrderFsCheck.verify(fsCheck).check(item1a.stat);
			inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
			inOrderFsCheck.verify(fsCheck).check(item1b.stat);
			inOrderFsCheck.verify(fsCheck).check(item2.stat);
			inOrderFsCheck.verify(fsCheck, org.mockito.Mockito.times(2)).check(item3.stat);
			inOrderFsCheck.verify(fsCheck).check(item5.stat);
			inOrderFsCheck.verify(fsCheck).check(item1b.stat);
			inOrderFsCheck.verify(fsCheck).check(item5.stat);
			inOrderFsCheck.verify(fsCheck).check(item5c.stat);
			inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
			inOrderFsCheck.verify(fsCheck).check(item5c.stat);
			inOrderFsCheck.verify(fsCheck, org.mockito.Mockito.times(2)).check(item5ca.stat);
			org.mockito.Mockito.verifyNoMoreInteractions(fsCheck);
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
			org.mockito.Mockito.verify(err).WriteLine("Infinite loop ignored: " + item5b.ToString
				() + " -> " + item5.ToString());
			org.mockito.Mockito.verifyNoMoreInteractions(err);
		}

		// check minimum depth is handledfollowLink
		/// <exception cref="System.IO.IOException"/>
		public virtual void processArgumentsMinDepth()
		{
			System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData> items = 
				createDirectories();
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.getOptions().setMinDepth(1);
			find.setConf(conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setOut(@out);
			System.IO.TextWriter err = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setErr(err);
			org.apache.hadoop.fs.shell.find.Expression expr = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(expr.apply((org.apache.hadoop.fs.shell.PathData)org.mockito.Matchers.any
				(), org.mockito.Matchers.anyInt())).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker fsCheck = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker>();
			org.apache.hadoop.fs.shell.find.Expression test = new org.apache.hadoop.fs.shell.find.TestFind.TestExpression
				(this, expr, fsCheck);
			find.setRootExpression(test);
			find.processArguments(items);
			org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(expr);
			inOrder.verify(expr).setOptions(find.getOptions());
			inOrder.verify(expr).prepare();
			inOrder.verify(expr).apply(item1a, 1);
			inOrder.verify(expr).apply(item1aa, 2);
			inOrder.verify(expr).apply(item1b, 1);
			inOrder.verify(expr).apply(item5a, 1);
			inOrder.verify(expr).apply(item5b, 1);
			inOrder.verify(expr).apply(item5c, 1);
			inOrder.verify(expr).apply(item5ca, 2);
			inOrder.verify(expr).apply(item5d, 1);
			inOrder.verify(expr).apply(item5e, 1);
			inOrder.verify(expr).finish();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
			org.mockito.InOrder inOrderFsCheck = org.mockito.Mockito.inOrder(fsCheck);
			inOrderFsCheck.verify(fsCheck).check(item1a.stat);
			inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
			inOrderFsCheck.verify(fsCheck).check(item1b.stat);
			inOrderFsCheck.verify(fsCheck).check(item5a.stat);
			inOrderFsCheck.verify(fsCheck).check(item5b.stat);
			inOrderFsCheck.verify(fsCheck).check(item5c.stat);
			inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
			inOrderFsCheck.verify(fsCheck).check(item5d.stat);
			inOrderFsCheck.verify(fsCheck).check(item5e.stat);
			org.mockito.Mockito.verifyNoMoreInteractions(fsCheck);
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
			org.mockito.Mockito.verifyNoMoreInteractions(err);
		}

		// check maximum depth is handled
		/// <exception cref="System.IO.IOException"/>
		public virtual void processArgumentsMaxDepth()
		{
			System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData> items = 
				createDirectories();
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.getOptions().setMaxDepth(1);
			find.setConf(conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setOut(@out);
			System.IO.TextWriter err = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setErr(err);
			org.apache.hadoop.fs.shell.find.Expression expr = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(expr.apply((org.apache.hadoop.fs.shell.PathData)org.mockito.Matchers.any
				(), org.mockito.Matchers.anyInt())).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker fsCheck = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker>();
			org.apache.hadoop.fs.shell.find.Expression test = new org.apache.hadoop.fs.shell.find.TestFind.TestExpression
				(this, expr, fsCheck);
			find.setRootExpression(test);
			find.processArguments(items);
			org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(expr);
			inOrder.verify(expr).setOptions(find.getOptions());
			inOrder.verify(expr).prepare();
			inOrder.verify(expr).apply(item1, 0);
			inOrder.verify(expr).apply(item1a, 1);
			inOrder.verify(expr).apply(item1b, 1);
			inOrder.verify(expr).apply(item2, 0);
			inOrder.verify(expr).apply(item3, 0);
			inOrder.verify(expr).apply(item4, 0);
			inOrder.verify(expr).apply(item5, 0);
			inOrder.verify(expr).apply(item5a, 1);
			inOrder.verify(expr).apply(item5b, 1);
			inOrder.verify(expr).apply(item5c, 1);
			inOrder.verify(expr).apply(item5d, 1);
			inOrder.verify(expr).apply(item5e, 1);
			inOrder.verify(expr).finish();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
			org.mockito.InOrder inOrderFsCheck = org.mockito.Mockito.inOrder(fsCheck);
			inOrderFsCheck.verify(fsCheck).check(item1.stat);
			inOrderFsCheck.verify(fsCheck).check(item1a.stat);
			inOrderFsCheck.verify(fsCheck).check(item1b.stat);
			inOrderFsCheck.verify(fsCheck).check(item2.stat);
			inOrderFsCheck.verify(fsCheck).check(item3.stat);
			inOrderFsCheck.verify(fsCheck).check(item4.stat);
			inOrderFsCheck.verify(fsCheck).check(item5.stat);
			inOrderFsCheck.verify(fsCheck).check(item5a.stat);
			inOrderFsCheck.verify(fsCheck).check(item5b.stat);
			inOrderFsCheck.verify(fsCheck).check(item5c.stat);
			inOrderFsCheck.verify(fsCheck).check(item5d.stat);
			inOrderFsCheck.verify(fsCheck).check(item5e.stat);
			org.mockito.Mockito.verifyNoMoreInteractions(fsCheck);
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
			org.mockito.Mockito.verifyNoMoreInteractions(err);
		}

		// check min depth is handled when -depth is specified
		/// <exception cref="System.IO.IOException"/>
		public virtual void processArgumentsDepthFirstMinDepth()
		{
			System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData> items = 
				createDirectories();
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.getOptions().setDepthFirst(true);
			find.getOptions().setMinDepth(1);
			find.setConf(conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setOut(@out);
			System.IO.TextWriter err = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setErr(err);
			org.apache.hadoop.fs.shell.find.Expression expr = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(expr.apply((org.apache.hadoop.fs.shell.PathData)org.mockito.Matchers.any
				(), org.mockito.Matchers.anyInt())).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker fsCheck = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker>();
			org.apache.hadoop.fs.shell.find.Expression test = new org.apache.hadoop.fs.shell.find.TestFind.TestExpression
				(this, expr, fsCheck);
			find.setRootExpression(test);
			find.processArguments(items);
			org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(expr);
			inOrder.verify(expr).setOptions(find.getOptions());
			inOrder.verify(expr).prepare();
			inOrder.verify(expr).apply(item1aa, 2);
			inOrder.verify(expr).apply(item1a, 1);
			inOrder.verify(expr).apply(item1b, 1);
			inOrder.verify(expr).apply(item5a, 1);
			inOrder.verify(expr).apply(item5b, 1);
			inOrder.verify(expr).apply(item5ca, 2);
			inOrder.verify(expr).apply(item5c, 1);
			inOrder.verify(expr).apply(item5d, 1);
			inOrder.verify(expr).apply(item5e, 1);
			inOrder.verify(expr).finish();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
			org.mockito.InOrder inOrderFsCheck = org.mockito.Mockito.inOrder(fsCheck);
			inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
			inOrderFsCheck.verify(fsCheck).check(item1a.stat);
			inOrderFsCheck.verify(fsCheck).check(item1b.stat);
			inOrderFsCheck.verify(fsCheck).check(item5a.stat);
			inOrderFsCheck.verify(fsCheck).check(item5b.stat);
			inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
			inOrderFsCheck.verify(fsCheck).check(item5c.stat);
			inOrderFsCheck.verify(fsCheck).check(item5d.stat);
			inOrderFsCheck.verify(fsCheck).check(item5e.stat);
			org.mockito.Mockito.verifyNoMoreInteractions(fsCheck);
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
			org.mockito.Mockito.verifyNoMoreInteractions(err);
		}

		// check max depth is handled when -depth is specified
		/// <exception cref="System.IO.IOException"/>
		public virtual void processArgumentsDepthFirstMaxDepth()
		{
			System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData> items = 
				createDirectories();
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.getOptions().setDepthFirst(true);
			find.getOptions().setMaxDepth(1);
			find.setConf(conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setOut(@out);
			System.IO.TextWriter err = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setErr(err);
			org.apache.hadoop.fs.shell.find.Expression expr = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(expr.apply((org.apache.hadoop.fs.shell.PathData)org.mockito.Matchers.any
				(), org.mockito.Matchers.anyInt())).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker fsCheck = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker>();
			org.apache.hadoop.fs.shell.find.Expression test = new org.apache.hadoop.fs.shell.find.TestFind.TestExpression
				(this, expr, fsCheck);
			find.setRootExpression(test);
			find.processArguments(items);
			org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(expr);
			inOrder.verify(expr).setOptions(find.getOptions());
			inOrder.verify(expr).prepare();
			inOrder.verify(expr).apply(item1a, 1);
			inOrder.verify(expr).apply(item1b, 1);
			inOrder.verify(expr).apply(item1, 0);
			inOrder.verify(expr).apply(item2, 0);
			inOrder.verify(expr).apply(item3, 0);
			inOrder.verify(expr).apply(item4, 0);
			inOrder.verify(expr).apply(item5a, 1);
			inOrder.verify(expr).apply(item5b, 1);
			inOrder.verify(expr).apply(item5c, 1);
			inOrder.verify(expr).apply(item5d, 1);
			inOrder.verify(expr).apply(item5e, 1);
			inOrder.verify(expr).apply(item5, 0);
			inOrder.verify(expr).finish();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
			org.mockito.InOrder inOrderFsCheck = org.mockito.Mockito.inOrder(fsCheck);
			inOrderFsCheck.verify(fsCheck).check(item1a.stat);
			inOrderFsCheck.verify(fsCheck).check(item1b.stat);
			inOrderFsCheck.verify(fsCheck).check(item1.stat);
			inOrderFsCheck.verify(fsCheck).check(item2.stat);
			inOrderFsCheck.verify(fsCheck).check(item3.stat);
			inOrderFsCheck.verify(fsCheck).check(item4.stat);
			inOrderFsCheck.verify(fsCheck).check(item5a.stat);
			inOrderFsCheck.verify(fsCheck).check(item5b.stat);
			inOrderFsCheck.verify(fsCheck).check(item5c.stat);
			inOrderFsCheck.verify(fsCheck).check(item5d.stat);
			inOrderFsCheck.verify(fsCheck).check(item5e.stat);
			inOrderFsCheck.verify(fsCheck).check(item5.stat);
			org.mockito.Mockito.verifyNoMoreInteractions(fsCheck);
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
			org.mockito.Mockito.verifyNoMoreInteractions(err);
		}

		// check expressions are called in the correct order
		/// <exception cref="System.IO.IOException"/>
		public virtual void processArgumentsNoDescend()
		{
			System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData> items = 
				createDirectories();
			org.apache.hadoop.fs.shell.find.Find find = new org.apache.hadoop.fs.shell.find.Find
				();
			find.setConf(conf);
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setOut(@out);
			System.IO.TextWriter err = org.mockito.Mockito.mock<System.IO.TextWriter>();
			find.getOptions().setErr(err);
			org.apache.hadoop.fs.shell.find.Expression expr = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(expr.apply((org.apache.hadoop.fs.shell.PathData)org.mockito.Matchers.any
				(), org.mockito.Matchers.anyInt())).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.mockito.Mockito.when(expr.apply(org.mockito.Matchers.eq(item1a), org.mockito.Matchers.anyInt
				())).thenReturn(org.apache.hadoop.fs.shell.find.Result.STOP);
			org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker fsCheck = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker>();
			org.apache.hadoop.fs.shell.find.Expression test = new org.apache.hadoop.fs.shell.find.TestFind.TestExpression
				(this, expr, fsCheck);
			find.setRootExpression(test);
			find.processArguments(items);
			org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(expr);
			inOrder.verify(expr).setOptions(find.getOptions());
			inOrder.verify(expr).prepare();
			inOrder.verify(expr).apply(item1, 0);
			inOrder.verify(expr).apply(item1a, 1);
			inOrder.verify(expr).apply(item1b, 1);
			inOrder.verify(expr).apply(item2, 0);
			inOrder.verify(expr).apply(item3, 0);
			inOrder.verify(expr).apply(item4, 0);
			inOrder.verify(expr).apply(item5, 0);
			inOrder.verify(expr).apply(item5a, 1);
			inOrder.verify(expr).apply(item5b, 1);
			inOrder.verify(expr).apply(item5c, 1);
			inOrder.verify(expr).apply(item5ca, 2);
			inOrder.verify(expr).apply(item5d, 1);
			inOrder.verify(expr).apply(item5e, 1);
			inOrder.verify(expr).finish();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
			org.mockito.InOrder inOrderFsCheck = org.mockito.Mockito.inOrder(fsCheck);
			inOrderFsCheck.verify(fsCheck).check(item1.stat);
			inOrderFsCheck.verify(fsCheck).check(item1a.stat);
			inOrderFsCheck.verify(fsCheck).check(item1b.stat);
			inOrderFsCheck.verify(fsCheck).check(item2.stat);
			inOrderFsCheck.verify(fsCheck).check(item3.stat);
			inOrderFsCheck.verify(fsCheck).check(item4.stat);
			inOrderFsCheck.verify(fsCheck).check(item5.stat);
			inOrderFsCheck.verify(fsCheck).check(item5a.stat);
			inOrderFsCheck.verify(fsCheck).check(item5b.stat);
			inOrderFsCheck.verify(fsCheck).check(item5c.stat);
			inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
			inOrderFsCheck.verify(fsCheck).check(item5d.stat);
			inOrderFsCheck.verify(fsCheck).check(item5e.stat);
			org.mockito.Mockito.verifyNoMoreInteractions(fsCheck);
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
			org.mockito.Mockito.verifyNoMoreInteractions(err);
		}

		private interface FileStatusChecker
		{
			void check(org.apache.hadoop.fs.FileStatus fileStatus);
		}

		private class TestExpression : org.apache.hadoop.fs.shell.find.BaseExpression, org.apache.hadoop.fs.shell.find.Expression
		{
			private org.apache.hadoop.fs.shell.find.Expression expr;

			private org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker checker;

			public TestExpression(TestFind _enclosing, org.apache.hadoop.fs.shell.find.Expression
				 expr, org.apache.hadoop.fs.shell.find.TestFind.FileStatusChecker checker)
			{
				this._enclosing = _enclosing;
				this.expr = expr;
				this.checker = checker;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.shell.find.Result apply(org.apache.hadoop.fs.shell.PathData
				 item, int depth)
			{
				org.apache.hadoop.fs.FileStatus fileStatus = this.getFileStatus(item, depth);
				this.checker.check(fileStatus);
				return this.expr.apply(item, depth);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void setOptions(org.apache.hadoop.fs.shell.find.FindOptions options
				)
			{
				base.setOptions(options);
				this.expr.setOptions(options);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void prepare()
			{
				this.expr.prepare();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void finish()
			{
				this.expr.finish();
			}

			private readonly TestFind _enclosing;
		}

		private org.apache.hadoop.fs.shell.PathData item1 = null;

		private org.apache.hadoop.fs.shell.PathData item1a = null;

		private org.apache.hadoop.fs.shell.PathData item1aa = null;

		private org.apache.hadoop.fs.shell.PathData item1b = null;

		private org.apache.hadoop.fs.shell.PathData item2 = null;

		private org.apache.hadoop.fs.shell.PathData item3 = null;

		private org.apache.hadoop.fs.shell.PathData item4 = null;

		private org.apache.hadoop.fs.shell.PathData item5 = null;

		private org.apache.hadoop.fs.shell.PathData item5a = null;

		private org.apache.hadoop.fs.shell.PathData item5b = null;

		private org.apache.hadoop.fs.shell.PathData item5c = null;

		private org.apache.hadoop.fs.shell.PathData item5ca = null;

		private org.apache.hadoop.fs.shell.PathData item5d = null;

		private org.apache.hadoop.fs.shell.PathData item5e = null;

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
		private System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData
			> createDirectories()
		{
			item1 = createPathData("item1");
			item1a = createPathData("item1/item1a");
			item1aa = createPathData("item1/item1a/item1aa");
			item1b = createPathData("item1/item1b");
			item2 = createPathData("item2");
			item3 = createPathData("item3");
			item4 = createPathData("item4");
			item5 = createPathData("item5");
			item5a = createPathData("item5/item5a");
			item5b = createPathData("item5/item5b");
			item5c = createPathData("item5/item5c");
			item5ca = createPathData("item5/item5c/item5ca");
			item5d = createPathData("item5/item5d");
			item5e = createPathData("item5/item5e");
			System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData> args = 
				new System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData>();
			org.mockito.Mockito.when(item1.stat.isDirectory()).thenReturn(true);
			org.mockito.Mockito.when(item1a.stat.isDirectory()).thenReturn(true);
			org.mockito.Mockito.when(item1aa.stat.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(item1b.stat.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(item2.stat.isDirectory()).thenReturn(true);
			org.mockito.Mockito.when(item3.stat.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(item4.stat.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(item5.stat.isDirectory()).thenReturn(true);
			org.mockito.Mockito.when(item5a.stat.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(item5b.stat.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(item5c.stat.isDirectory()).thenReturn(true);
			org.mockito.Mockito.when(item5ca.stat.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(item5d.stat.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(item5e.stat.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(mockFs.listStatus(org.mockito.Matchers.eq(item1.path))).
				thenReturn(new org.apache.hadoop.fs.FileStatus[] { item1a.stat, item1b.stat });
			org.mockito.Mockito.when(mockFs.listStatus(org.mockito.Matchers.eq(item1a.path)))
				.thenReturn(new org.apache.hadoop.fs.FileStatus[] { item1aa.stat });
			org.mockito.Mockito.when(mockFs.listStatus(org.mockito.Matchers.eq(item2.path))).
				thenReturn(new org.apache.hadoop.fs.FileStatus[0]);
			org.mockito.Mockito.when(mockFs.listStatus(org.mockito.Matchers.eq(item5.path))).
				thenReturn(new org.apache.hadoop.fs.FileStatus[] { item5a.stat, item5b.stat, item5c
				.stat, item5d.stat, item5e.stat });
			org.mockito.Mockito.when(mockFs.listStatus(org.mockito.Matchers.eq(item5c.path)))
				.thenReturn(new org.apache.hadoop.fs.FileStatus[] { item5ca.stat });
			org.mockito.Mockito.when(item1.stat.isSymlink()).thenReturn(false);
			org.mockito.Mockito.when(item1a.stat.isSymlink()).thenReturn(false);
			org.mockito.Mockito.when(item1aa.stat.isSymlink()).thenReturn(false);
			org.mockito.Mockito.when(item1b.stat.isSymlink()).thenReturn(false);
			org.mockito.Mockito.when(item2.stat.isSymlink()).thenReturn(false);
			org.mockito.Mockito.when(item3.stat.isSymlink()).thenReturn(false);
			org.mockito.Mockito.when(item4.stat.isSymlink()).thenReturn(true);
			org.mockito.Mockito.when(item5.stat.isSymlink()).thenReturn(false);
			org.mockito.Mockito.when(item5a.stat.isSymlink()).thenReturn(true);
			org.mockito.Mockito.when(item5b.stat.isSymlink()).thenReturn(true);
			org.mockito.Mockito.when(item5d.stat.isSymlink()).thenReturn(true);
			org.mockito.Mockito.when(item5e.stat.isSymlink()).thenReturn(true);
			org.mockito.Mockito.when(item4.stat.getSymlink()).thenReturn(item3.path);
			org.mockito.Mockito.when(item5a.stat.getSymlink()).thenReturn(item1b.path);
			org.mockito.Mockito.when(item5b.stat.getSymlink()).thenReturn(item5.path);
			org.mockito.Mockito.when(item5d.stat.getSymlink()).thenReturn(item5c.path);
			org.mockito.Mockito.when(item5e.stat.getSymlink()).thenReturn(item5ca.path);
			args.add(item1);
			args.add(item2);
			args.add(item3);
			args.add(item4);
			args.add(item5);
			return args;
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.shell.PathData createPathData(string name)
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(name);
			org.apache.hadoop.fs.FileStatus fstat = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileStatus
				>();
			org.mockito.Mockito.when(fstat.getPath()).thenReturn(path);
			org.mockito.Mockito.when(fstat.ToString()).thenReturn("fileStatus:" + name);
			org.mockito.Mockito.when(mockFs.getFileStatus(org.mockito.Matchers.eq(path))).thenReturn
				(fstat);
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				(path.ToString(), conf);
			return item;
		}

		private System.Collections.Generic.LinkedList<string> getArgs(string cmd)
		{
			return new System.Collections.Generic.LinkedList<string>(java.util.Arrays.asList(
				cmd.split(" ")));
		}
	}
}
