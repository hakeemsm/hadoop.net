using Org.Apache.Hadoop.FS.Shell;


namespace Org.Apache.Hadoop.FS.Shell.Find
{
	/// <summary>
	/// Interface describing an expression to be used in the
	/// <see cref="Find"/>
	/// command.
	/// </summary>
	public interface Expression
	{
		/// <summary>
		/// Set the options for this expression, called once before processing any
		/// items.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		void SetOptions(FindOptions options);

		/// <summary>
		/// Prepares the expression for execution, called once after setting options
		/// and before processing any options.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		void Prepare();

		/// <summary>Apply the expression to the specified item, called once for each item.</summary>
		/// <param name="item">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.Shell.PathData"/>
		/// item to be processed
		/// </param>
		/// <param name="depth">distance of the item from the command line argument</param>
		/// <returns>
		/// 
		/// <see cref="Result"/>
		/// of applying the expression to the item
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		Result Apply(PathData item, int depth);

		/// <summary>Finishes the expression, called once after processing all items.</summary>
		/// <exception cref="System.IO.IOException"/>
		void Finish();

		/// <summary>Returns brief usage instructions for this expression.</summary>
		/// <remarks>
		/// Returns brief usage instructions for this expression. Multiple items should
		/// be returned if there are multiple ways to use this expression.
		/// </remarks>
		/// <returns>array of usage instructions</returns>
		string[] GetUsage();

		/// <summary>Returns a description of the expression for use in help.</summary>
		/// <remarks>
		/// Returns a description of the expression for use in help. Multiple lines
		/// should be returned array items. Lines should be formated to 60 characters
		/// or less.
		/// </remarks>
		/// <returns>array of description lines</returns>
		string[] GetHelp();

		/// <summary>Indicates whether this expression performs an action, i.e.</summary>
		/// <remarks>
		/// Indicates whether this expression performs an action, i.e. provides output
		/// back to the user.
		/// </remarks>
		bool IsAction();

		/// <summary>Identifies the expression as an operator rather than a primary.</summary>
		bool IsOperator();

		/// <summary>
		/// Returns the precedence of this expression
		/// (only applicable to operators).
		/// </summary>
		int GetPrecedence();

		/// <summary>Adds children to this expression.</summary>
		/// <remarks>
		/// Adds children to this expression. Children are popped from the head of the
		/// deque.
		/// </remarks>
		/// <param name="expressions">deque of expressions from which to take the children</param>
		void AddChildren(Deque<Expression> expressions);

		/// <summary>Adds arguments to this expression.</summary>
		/// <remarks>
		/// Adds arguments to this expression. Arguments are popped from the head of
		/// the deque and added to the front of the child list, ie last child added is
		/// the first evaluated.
		/// </remarks>
		/// <param name="args">deque of arguments from which to take expression arguments</param>
		void AddArguments(Deque<string> args);
	}
}
