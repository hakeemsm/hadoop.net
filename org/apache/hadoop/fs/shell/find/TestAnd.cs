using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	public class TestAnd
	{
		// test all expressions passing
		/// <exception cref="System.IO.IOException"/>
		public virtual void testPass()
		{
			org.apache.hadoop.fs.shell.find.And and = new org.apache.hadoop.fs.shell.find.And
				();
			org.apache.hadoop.fs.shell.PathData pathData = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.PathData
				>();
			org.apache.hadoop.fs.shell.find.Expression first = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(first.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.apache.hadoop.fs.shell.find.Expression second = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(second.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> children = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			children.add(second);
			children.add(first);
			and.addChildren(children);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.PASS, and.
				apply(pathData, -1));
			org.mockito.Mockito.verify(first).apply(pathData, -1);
			org.mockito.Mockito.verify(second).apply(pathData, -1);
			org.mockito.Mockito.verifyNoMoreInteractions(first);
			org.mockito.Mockito.verifyNoMoreInteractions(second);
		}

		// test the first expression failing
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailFirst()
		{
			org.apache.hadoop.fs.shell.find.And and = new org.apache.hadoop.fs.shell.find.And
				();
			org.apache.hadoop.fs.shell.PathData pathData = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.PathData
				>();
			org.apache.hadoop.fs.shell.find.Expression first = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(first.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.FAIL);
			org.apache.hadoop.fs.shell.find.Expression second = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(second.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> children = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			children.add(second);
			children.add(first);
			and.addChildren(children);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.FAIL, and.
				apply(pathData, -1));
			org.mockito.Mockito.verify(first).apply(pathData, -1);
			org.mockito.Mockito.verifyNoMoreInteractions(first);
			org.mockito.Mockito.verifyNoMoreInteractions(second);
		}

		// test the second expression failing
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailSecond()
		{
			org.apache.hadoop.fs.shell.find.And and = new org.apache.hadoop.fs.shell.find.And
				();
			org.apache.hadoop.fs.shell.PathData pathData = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.PathData
				>();
			org.apache.hadoop.fs.shell.find.Expression first = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(first.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.apache.hadoop.fs.shell.find.Expression second = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(second.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.FAIL);
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> children = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			children.add(second);
			children.add(first);
			and.addChildren(children);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.FAIL, and.
				apply(pathData, -1));
			org.mockito.Mockito.verify(first).apply(pathData, -1);
			org.mockito.Mockito.verify(second).apply(pathData, -1);
			org.mockito.Mockito.verifyNoMoreInteractions(first);
			org.mockito.Mockito.verifyNoMoreInteractions(second);
		}

		// test both expressions failing
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailBoth()
		{
			org.apache.hadoop.fs.shell.find.And and = new org.apache.hadoop.fs.shell.find.And
				();
			org.apache.hadoop.fs.shell.PathData pathData = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.PathData
				>();
			org.apache.hadoop.fs.shell.find.Expression first = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(first.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.FAIL);
			org.apache.hadoop.fs.shell.find.Expression second = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(second.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.FAIL);
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> children = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			children.add(second);
			children.add(first);
			and.addChildren(children);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.FAIL, and.
				apply(pathData, -1));
			org.mockito.Mockito.verify(first).apply(pathData, -1);
			org.mockito.Mockito.verifyNoMoreInteractions(first);
			org.mockito.Mockito.verifyNoMoreInteractions(second);
		}

		// test the first expression stopping
		/// <exception cref="System.IO.IOException"/>
		public virtual void testStopFirst()
		{
			org.apache.hadoop.fs.shell.find.And and = new org.apache.hadoop.fs.shell.find.And
				();
			org.apache.hadoop.fs.shell.PathData pathData = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.PathData
				>();
			org.apache.hadoop.fs.shell.find.Expression first = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(first.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.STOP);
			org.apache.hadoop.fs.shell.find.Expression second = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(second.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> children = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			children.add(second);
			children.add(first);
			and.addChildren(children);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.STOP, and.
				apply(pathData, -1));
			org.mockito.Mockito.verify(first).apply(pathData, -1);
			org.mockito.Mockito.verify(second).apply(pathData, -1);
			org.mockito.Mockito.verifyNoMoreInteractions(first);
			org.mockito.Mockito.verifyNoMoreInteractions(second);
		}

		// test the second expression stopping
		/// <exception cref="System.IO.IOException"/>
		public virtual void testStopSecond()
		{
			org.apache.hadoop.fs.shell.find.And and = new org.apache.hadoop.fs.shell.find.And
				();
			org.apache.hadoop.fs.shell.PathData pathData = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.PathData
				>();
			org.apache.hadoop.fs.shell.find.Expression first = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(first.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.PASS);
			org.apache.hadoop.fs.shell.find.Expression second = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(second.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.STOP);
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> children = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			children.add(second);
			children.add(first);
			and.addChildren(children);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.STOP, and.
				apply(pathData, -1));
			org.mockito.Mockito.verify(first).apply(pathData, -1);
			org.mockito.Mockito.verify(second).apply(pathData, -1);
			org.mockito.Mockito.verifyNoMoreInteractions(first);
			org.mockito.Mockito.verifyNoMoreInteractions(second);
		}

		// test first expression stopping and second failing
		/// <exception cref="System.IO.IOException"/>
		public virtual void testStopFail()
		{
			org.apache.hadoop.fs.shell.find.And and = new org.apache.hadoop.fs.shell.find.And
				();
			org.apache.hadoop.fs.shell.PathData pathData = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.PathData
				>();
			org.apache.hadoop.fs.shell.find.Expression first = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(first.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.STOP);
			org.apache.hadoop.fs.shell.find.Expression second = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.mockito.Mockito.when(second.apply(pathData, -1)).thenReturn(org.apache.hadoop.fs.shell.find.Result
				.FAIL);
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> children = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			children.add(second);
			children.add(first);
			and.addChildren(children);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.STOP.combine
				(org.apache.hadoop.fs.shell.find.Result.FAIL), and.apply(pathData, -1));
			org.mockito.Mockito.verify(first).apply(pathData, -1);
			org.mockito.Mockito.verify(second).apply(pathData, -1);
			org.mockito.Mockito.verifyNoMoreInteractions(first);
			org.mockito.Mockito.verifyNoMoreInteractions(second);
		}

		// test setOptions is called on child
		/// <exception cref="System.IO.IOException"/>
		public virtual void testSetOptions()
		{
			org.apache.hadoop.fs.shell.find.And and = new org.apache.hadoop.fs.shell.find.And
				();
			org.apache.hadoop.fs.shell.find.Expression first = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.apache.hadoop.fs.shell.find.Expression second = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> children = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			children.add(second);
			children.add(first);
			and.addChildren(children);
			org.apache.hadoop.fs.shell.find.FindOptions options = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.FindOptions
				>();
			and.setOptions(options);
			org.mockito.Mockito.verify(first).setOptions(options);
			org.mockito.Mockito.verify(second).setOptions(options);
			org.mockito.Mockito.verifyNoMoreInteractions(first);
			org.mockito.Mockito.verifyNoMoreInteractions(second);
		}

		// test prepare is called on child
		/// <exception cref="System.IO.IOException"/>
		public virtual void testPrepare()
		{
			org.apache.hadoop.fs.shell.find.And and = new org.apache.hadoop.fs.shell.find.And
				();
			org.apache.hadoop.fs.shell.find.Expression first = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.apache.hadoop.fs.shell.find.Expression second = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> children = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			children.add(second);
			children.add(first);
			and.addChildren(children);
			and.prepare();
			org.mockito.Mockito.verify(first).prepare();
			org.mockito.Mockito.verify(second).prepare();
			org.mockito.Mockito.verifyNoMoreInteractions(first);
			org.mockito.Mockito.verifyNoMoreInteractions(second);
		}

		// test finish is called on child
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFinish()
		{
			org.apache.hadoop.fs.shell.find.And and = new org.apache.hadoop.fs.shell.find.And
				();
			org.apache.hadoop.fs.shell.find.Expression first = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			org.apache.hadoop.fs.shell.find.Expression second = org.mockito.Mockito.mock<org.apache.hadoop.fs.shell.find.Expression
				>();
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> children = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			children.add(second);
			children.add(first);
			and.addChildren(children);
			and.finish();
			org.mockito.Mockito.verify(first).finish();
			org.mockito.Mockito.verify(second).finish();
			org.mockito.Mockito.verifyNoMoreInteractions(first);
			org.mockito.Mockito.verifyNoMoreInteractions(second);
		}
	}
}
