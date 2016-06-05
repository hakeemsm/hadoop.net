using System.Collections.Generic;
using Org.Apache.Hadoop.FS.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	public class TestAnd
	{
		// test all expressions passing
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPass()
		{
			And and = new And();
			PathData pathData = Org.Mockito.Mockito.Mock<PathData>();
			Expression first = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(first.Apply(pathData, -1)).ThenReturn(Result.Pass);
			Expression second = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(second.Apply(pathData, -1)).ThenReturn(Result.Pass);
			Deque<Expression> children = new List<Expression>();
			children.AddItem(second);
			children.AddItem(first);
			and.AddChildren(children);
			Assert.Equal(Result.Pass, and.Apply(pathData, -1));
			Org.Mockito.Mockito.Verify(first).Apply(pathData, -1);
			Org.Mockito.Mockito.Verify(second).Apply(pathData, -1);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(first);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(second);
		}

		// test the first expression failing
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailFirst()
		{
			And and = new And();
			PathData pathData = Org.Mockito.Mockito.Mock<PathData>();
			Expression first = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(first.Apply(pathData, -1)).ThenReturn(Result.Fail);
			Expression second = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(second.Apply(pathData, -1)).ThenReturn(Result.Pass);
			Deque<Expression> children = new List<Expression>();
			children.AddItem(second);
			children.AddItem(first);
			and.AddChildren(children);
			Assert.Equal(Result.Fail, and.Apply(pathData, -1));
			Org.Mockito.Mockito.Verify(first).Apply(pathData, -1);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(first);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(second);
		}

		// test the second expression failing
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailSecond()
		{
			And and = new And();
			PathData pathData = Org.Mockito.Mockito.Mock<PathData>();
			Expression first = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(first.Apply(pathData, -1)).ThenReturn(Result.Pass);
			Expression second = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(second.Apply(pathData, -1)).ThenReturn(Result.Fail);
			Deque<Expression> children = new List<Expression>();
			children.AddItem(second);
			children.AddItem(first);
			and.AddChildren(children);
			Assert.Equal(Result.Fail, and.Apply(pathData, -1));
			Org.Mockito.Mockito.Verify(first).Apply(pathData, -1);
			Org.Mockito.Mockito.Verify(second).Apply(pathData, -1);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(first);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(second);
		}

		// test both expressions failing
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailBoth()
		{
			And and = new And();
			PathData pathData = Org.Mockito.Mockito.Mock<PathData>();
			Expression first = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(first.Apply(pathData, -1)).ThenReturn(Result.Fail);
			Expression second = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(second.Apply(pathData, -1)).ThenReturn(Result.Fail);
			Deque<Expression> children = new List<Expression>();
			children.AddItem(second);
			children.AddItem(first);
			and.AddChildren(children);
			Assert.Equal(Result.Fail, and.Apply(pathData, -1));
			Org.Mockito.Mockito.Verify(first).Apply(pathData, -1);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(first);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(second);
		}

		// test the first expression stopping
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStopFirst()
		{
			And and = new And();
			PathData pathData = Org.Mockito.Mockito.Mock<PathData>();
			Expression first = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(first.Apply(pathData, -1)).ThenReturn(Result.Stop);
			Expression second = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(second.Apply(pathData, -1)).ThenReturn(Result.Pass);
			Deque<Expression> children = new List<Expression>();
			children.AddItem(second);
			children.AddItem(first);
			and.AddChildren(children);
			Assert.Equal(Result.Stop, and.Apply(pathData, -1));
			Org.Mockito.Mockito.Verify(first).Apply(pathData, -1);
			Org.Mockito.Mockito.Verify(second).Apply(pathData, -1);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(first);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(second);
		}

		// test the second expression stopping
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStopSecond()
		{
			And and = new And();
			PathData pathData = Org.Mockito.Mockito.Mock<PathData>();
			Expression first = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(first.Apply(pathData, -1)).ThenReturn(Result.Pass);
			Expression second = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(second.Apply(pathData, -1)).ThenReturn(Result.Stop);
			Deque<Expression> children = new List<Expression>();
			children.AddItem(second);
			children.AddItem(first);
			and.AddChildren(children);
			Assert.Equal(Result.Stop, and.Apply(pathData, -1));
			Org.Mockito.Mockito.Verify(first).Apply(pathData, -1);
			Org.Mockito.Mockito.Verify(second).Apply(pathData, -1);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(first);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(second);
		}

		// test first expression stopping and second failing
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStopFail()
		{
			And and = new And();
			PathData pathData = Org.Mockito.Mockito.Mock<PathData>();
			Expression first = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(first.Apply(pathData, -1)).ThenReturn(Result.Stop);
			Expression second = Org.Mockito.Mockito.Mock<Expression>();
			Org.Mockito.Mockito.When(second.Apply(pathData, -1)).ThenReturn(Result.Fail);
			Deque<Expression> children = new List<Expression>();
			children.AddItem(second);
			children.AddItem(first);
			and.AddChildren(children);
			Assert.Equal(Result.Stop.Combine(Result.Fail), and.Apply(pathData
				, -1));
			Org.Mockito.Mockito.Verify(first).Apply(pathData, -1);
			Org.Mockito.Mockito.Verify(second).Apply(pathData, -1);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(first);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(second);
		}

		// test setOptions is called on child
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetOptions()
		{
			And and = new And();
			Expression first = Org.Mockito.Mockito.Mock<Expression>();
			Expression second = Org.Mockito.Mockito.Mock<Expression>();
			Deque<Expression> children = new List<Expression>();
			children.AddItem(second);
			children.AddItem(first);
			and.AddChildren(children);
			FindOptions options = Org.Mockito.Mockito.Mock<FindOptions>();
			and.SetOptions(options);
			Org.Mockito.Mockito.Verify(first).SetOptions(options);
			Org.Mockito.Mockito.Verify(second).SetOptions(options);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(first);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(second);
		}

		// test prepare is called on child
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPrepare()
		{
			And and = new And();
			Expression first = Org.Mockito.Mockito.Mock<Expression>();
			Expression second = Org.Mockito.Mockito.Mock<Expression>();
			Deque<Expression> children = new List<Expression>();
			children.AddItem(second);
			children.AddItem(first);
			and.AddChildren(children);
			and.Prepare();
			Org.Mockito.Mockito.Verify(first).Prepare();
			Org.Mockito.Mockito.Verify(second).Prepare();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(first);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(second);
		}

		// test finish is called on child
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFinish()
		{
			And and = new And();
			Expression first = Org.Mockito.Mockito.Mock<Expression>();
			Expression second = Org.Mockito.Mockito.Mock<Expression>();
			Deque<Expression> children = new List<Expression>();
			children.AddItem(second);
			children.AddItem(first);
			and.AddChildren(children);
			and.Finish();
			Org.Mockito.Mockito.Verify(first).Finish();
			Org.Mockito.Mockito.Verify(second).Finish();
			Org.Mockito.Mockito.VerifyNoMoreInteractions(first);
			Org.Mockito.Mockito.VerifyNoMoreInteractions(second);
		}
	}
}
