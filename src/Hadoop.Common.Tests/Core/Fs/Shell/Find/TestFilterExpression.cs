using NUnit.Framework;
using Org.Apache.Hadoop.FS.Shell;


namespace Org.Apache.Hadoop.FS.Shell.Find
{
	public class TestFilterExpression
	{
		private Org.Apache.Hadoop.FS.Shell.Find.Expression expr;

		private FilterExpression test;

		[SetUp]
		public virtual void Setup()
		{
			expr = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.FS.Shell.Find.Expression>();
			test = new _FilterExpression_38(expr);
		}

		private sealed class _FilterExpression_38 : FilterExpression
		{
			public _FilterExpression_38(Org.Apache.Hadoop.FS.Shell.Find.Expression baseArg1)
				: base(baseArg1)
			{
			}
		}

		// test that the child expression is correctly set
		/// <exception cref="System.IO.IOException"/>
		public virtual void Expression()
		{
			Assert.Equal(expr, test.expression);
		}

		// test that setOptions method is called
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetOptions()
		{
			FindOptions options = Org.Mockito.Mockito.Mock<FindOptions>();
			test.SetOptions(options);
			Org.Mockito.Mockito.Verify(expr).SetOptions(options);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
		}

		// test the apply method is called and the result returned
		/// <exception cref="System.IO.IOException"/>
		public virtual void Apply()
		{
			PathData item = Org.Mockito.Mockito.Mock<PathData>();
			Org.Mockito.Mockito.When(expr.Apply(item, -1)).ThenReturn(Result.Pass).ThenReturn
				(Result.Fail);
			Assert.Equal(Result.Pass, test.Apply(item, -1));
			Assert.Equal(Result.Fail, test.Apply(item, -1));
			Org.Mockito.Mockito.Verify(expr, Org.Mockito.Mockito.Times(2)).Apply(item, -1);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
		}

		// test that the finish method is called
		/// <exception cref="System.IO.IOException"/>
		public virtual void Finish()
		{
			test.Finish();
			Org.Mockito.Mockito.Verify(expr).Finish();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
		}

		// test that the getUsage method is called
		public virtual void GetUsage()
		{
			string[] usage = new string[] { "Usage 1", "Usage 2", "Usage 3" };
			Org.Mockito.Mockito.When(expr.GetUsage()).ThenReturn(usage);
			Assert.AssertArrayEquals(usage, test.GetUsage());
			Org.Mockito.Mockito.Verify(expr).GetUsage();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
		}

		// test that the getHelp method is called
		public virtual void GetHelp()
		{
			string[] help = new string[] { "Help 1", "Help 2", "Help 3" };
			Org.Mockito.Mockito.When(expr.GetHelp()).ThenReturn(help);
			Assert.AssertArrayEquals(help, test.GetHelp());
			Org.Mockito.Mockito.Verify(expr).GetHelp();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
		}

		// test that the isAction method is called
		public virtual void IsAction()
		{
			Org.Mockito.Mockito.When(expr.IsAction()).ThenReturn(true).ThenReturn(false);
			Assert.True(test.IsAction());
			NUnit.Framework.Assert.IsFalse(test.IsAction());
			Org.Mockito.Mockito.Verify(expr, Org.Mockito.Mockito.Times(2)).IsAction();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
		}

		// test that the isOperator method is called
		public virtual void IsOperator()
		{
			Org.Mockito.Mockito.When(expr.IsAction()).ThenReturn(true).ThenReturn(false);
			Assert.True(test.IsAction());
			NUnit.Framework.Assert.IsFalse(test.IsAction());
			Org.Mockito.Mockito.Verify(expr, Org.Mockito.Mockito.Times(2)).IsAction();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
		}

		// test that the getPrecedence method is called
		public virtual void GetPrecedence()
		{
			int precedence = 12345;
			Org.Mockito.Mockito.When(expr.GetPrecedence()).ThenReturn(precedence);
			Assert.Equal(precedence, test.GetPrecedence());
			Org.Mockito.Mockito.Verify(expr).GetPrecedence();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
		}

		// test that the addChildren method is called
		public virtual void AddChildren()
		{
			Deque<Org.Apache.Hadoop.FS.Shell.Find.Expression> expressions = Org.Mockito.Mockito.Mock
				<Deque>();
			test.AddChildren(expressions);
			Org.Mockito.Mockito.Verify(expr).AddChildren(expressions);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
		}

		// test that the addArguments method is called
		public virtual void AddArguments()
		{
			Deque<string> args = Org.Mockito.Mockito.Mock<Deque>();
			test.AddArguments(args);
			Org.Mockito.Mockito.Verify(expr).AddArguments(args);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(expr);
		}
	}
}
