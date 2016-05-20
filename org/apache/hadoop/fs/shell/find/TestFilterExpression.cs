using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	public class TestFilterExpression
	{
		private org.apache.hadoop.fs.shell.find.Expression expr;

		private org.apache.hadoop.fs.shell.find.FilterExpression test;

		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			expr = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression>();
			test = new _FilterExpression_38(expr);
		}

		private sealed class _FilterExpression_38 : org.apache.hadoop.fs.shell.find.FilterExpression
		{
			public _FilterExpression_38(org.apache.hadoop.fs.shell.find.Expression baseArg1)
				: base(baseArg1)
			{
			}
		}

		// test that the child expression is correctly set
		/// <exception cref="System.IO.IOException"/>
		public virtual void expression()
		{
			NUnit.Framework.Assert.AreEqual(expr, test.expression);
		}

		// test that setOptions method is called
		/// <exception cref="System.IO.IOException"/>
		public virtual void setOptions()
		{
			org.apache.hadoop.fs.shell.find.FindOptions options = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.FindOptions
				>();
			test.setOptions(options);
			org.mockito.Mockito.verify(expr).setOptions(options);
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
		}

		// test the apply method is called and the result returned
		/// <exception cref="System.IO.IOException"/>
		public virtual void apply()
		{
			org.apache.hadoop.fs.shell.PathData item = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.PathData
				>();
			org.mockito.Mockito.when(expr.apply(item, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS).thenReturn(org.apache.hadoop.fs.shell.find.Result.FAIL);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.PASS, test
				.apply(item, -1));
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.FAIL, test
				.apply(item, -1));
			org.mockito.Mockito.verify(expr, org.mockito.Mockito.times(2)).apply(item, -1);
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
		}

		// test that the finish method is called
		/// <exception cref="System.IO.IOException"/>
		public virtual void finish()
		{
			test.finish();
			org.mockito.Mockito.verify(expr).finish();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
		}

		// test that the getUsage method is called
		public virtual void getUsage()
		{
			string[] usage = new string[] { "Usage 1", "Usage 2", "Usage 3" };
			org.mockito.Mockito.when(expr.getUsage()).thenReturn(usage);
			NUnit.Framework.Assert.assertArrayEquals(usage, test.getUsage());
			org.mockito.Mockito.verify(expr).getUsage();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
		}

		// test that the getHelp method is called
		public virtual void getHelp()
		{
			string[] help = new string[] { "Help 1", "Help 2", "Help 3" };
			org.mockito.Mockito.when(expr.getHelp()).thenReturn(help);
			NUnit.Framework.Assert.assertArrayEquals(help, test.getHelp());
			org.mockito.Mockito.verify(expr).getHelp();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
		}

		// test that the isAction method is called
		public virtual void isAction()
		{
			org.mockito.Mockito.when(expr.isAction()).thenReturn(true).thenReturn(false);
			NUnit.Framework.Assert.IsTrue(test.isAction());
			NUnit.Framework.Assert.IsFalse(test.isAction());
			org.mockito.Mockito.verify(expr, org.mockito.Mockito.times(2)).isAction();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
		}

		// test that the isOperator method is called
		public virtual void isOperator()
		{
			org.mockito.Mockito.when(expr.isAction()).thenReturn(true).thenReturn(false);
			NUnit.Framework.Assert.IsTrue(test.isAction());
			NUnit.Framework.Assert.IsFalse(test.isAction());
			org.mockito.Mockito.verify(expr, org.mockito.Mockito.times(2)).isAction();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
		}

		// test that the getPrecedence method is called
		public virtual void getPrecedence()
		{
			int precedence = 12345;
			org.mockito.Mockito.when(expr.getPrecedence()).thenReturn(precedence);
			NUnit.Framework.Assert.AreEqual(precedence, test.getPrecedence());
			org.mockito.Mockito.verify(expr).getPrecedence();
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
		}

		// test that the addChildren method is called
		public virtual void addChildren()
		{
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> expressions = org.mockito.Mockito.mock
				<java.util.Deque>();
			test.addChildren(expressions);
			org.mockito.Mockito.verify(expr).addChildren(expressions);
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
		}

		// test that the addArguments method is called
		public virtual void addArguments()
		{
			java.util.Deque<string> args = org.mockito.Mockito.mock<java.util.Deque>();
			test.addArguments(args);
			org.mockito.Mockito.verify(expr).addArguments(args);
			org.mockito.Mockito.verifyNoMoreInteractions(expr);
		}
	}
}
