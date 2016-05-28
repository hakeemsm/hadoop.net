using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>Very simple shift-reduce parser for join expressions.</summary>
	/// <remarks>
	/// Very simple shift-reduce parser for join expressions.
	/// This should be sufficient for the user extension permitted now, but ought to
	/// be replaced with a parser generator if more complex grammars are supported.
	/// In particular, this &quot;shift-reduce&quot; parser has no states. Each set
	/// of formals requires a different internal node type, which is responsible for
	/// interpreting the list of tokens it receives. This is sufficient for the
	/// current grammar, but it has several annoying properties that might inhibit
	/// extension. In particular, parenthesis are always function calls; an
	/// algebraic or filter grammar would not only require a node type, but must
	/// also work around the internals of this parser.
	/// For most other cases, adding classes to the hierarchy- particularly by
	/// extending JoinRecordReader and MultiFilterRecordReader- is fairly
	/// straightforward. One need only override the relevant method(s) (usually only
	/// <see cref="CompositeRecordReader{K, V, X}.Combine(object[], TupleWritable)"/>
	/// ) and include a property to map its
	/// value to an identifier in the parser.
	/// </remarks>
	public class Parser
	{
		public enum TType
		{
			Cif,
			Ident,
			Comma,
			Lparen,
			Rparen,
			Quot,
			Num
		}

		/// <summary>Tagged-union type for tokens from the join expression.</summary>
		/// <seealso cref="TType"/>
		public class Token
		{
			private Parser.TType type;

			internal Token(Parser.TType type)
			{
				this.type = type;
			}

			public virtual Parser.TType GetType()
			{
				return type;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual Parser.Node GetNode()
			{
				throw new IOException("Expected nodetype");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual double GetNum()
			{
				throw new IOException("Expected numtype");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string GetStr()
			{
				throw new IOException("Expected strtype");
			}
		}

		public class NumToken : Parser.Token
		{
			private double num;

			public NumToken(double num)
				: base(Parser.TType.Num)
			{
				this.num = num;
			}

			public override double GetNum()
			{
				return num;
			}
		}

		public class NodeToken : Parser.Token
		{
			private Parser.Node node;

			internal NodeToken(Parser.Node node)
				: base(Parser.TType.Cif)
			{
				this.node = node;
			}

			public override Parser.Node GetNode()
			{
				return node;
			}
		}

		public class StrToken : Parser.Token
		{
			private string str;

			public StrToken(Parser.TType type, string str)
				: base(type)
			{
				this.str = str;
			}

			public override string GetStr()
			{
				return str;
			}
		}

		/// <summary>Simple lexer wrapping a StreamTokenizer.</summary>
		/// <remarks>
		/// Simple lexer wrapping a StreamTokenizer.
		/// This encapsulates the creation of tagged-union Tokens and initializes the
		/// SteamTokenizer.
		/// </remarks>
		private class Lexer
		{
			private StreamTokenizer tok;

			internal Lexer(string s)
			{
				tok = new StreamTokenizer(new CharArrayReader(s.ToCharArray()));
				tok.QuoteChar('"');
				tok.ParseNumbers();
				tok.OrdinaryChar(',');
				tok.OrdinaryChar('(');
				tok.OrdinaryChar(')');
				tok.WordChars('$', '$');
				tok.WordChars('_', '_');
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual Parser.Token Next()
			{
				int type = tok.NextToken();
				switch (type)
				{
					case StreamTokenizer.TtEof:
					case StreamTokenizer.TtEol:
					{
						return null;
					}

					case StreamTokenizer.TtNumber:
					{
						return new Parser.NumToken(tok.nval);
					}

					case StreamTokenizer.TtWord:
					{
						return new Parser.StrToken(Parser.TType.Ident, tok.sval);
					}

					case '"':
					{
						return new Parser.StrToken(Parser.TType.Quot, tok.sval);
					}

					default:
					{
						switch (type)
						{
							case ',':
							{
								return new Parser.Token(Parser.TType.Comma);
							}

							case '(':
							{
								return new Parser.Token(Parser.TType.Lparen);
							}

							case ')':
							{
								return new Parser.Token(Parser.TType.Rparen);
							}

							default:
							{
								throw new IOException("Unexpected: " + type);
							}
						}
						break;
					}
				}
			}
		}

		public abstract class Node : ComposableInputFormat
		{
			/// <summary>Return the node type registered for the particular identifier.</summary>
			/// <remarks>
			/// Return the node type registered for the particular identifier.
			/// By default, this is a CNode for any composite node and a WNode
			/// for &quot;wrapped&quot; nodes. User nodes will likely be composite
			/// nodes.
			/// </remarks>
			/// <seealso cref="AddIdentifier(string, System.Type{T}[], System.Type{T}, System.Type{T})
			/// 	"/>
			/// <seealso cref="CompositeInputFormat{K}.SetFormat(Org.Apache.Hadoop.Conf.Configuration)
			/// 	"/>
			/// <exception cref="System.IO.IOException"/>
			internal static Parser.Node ForIdent(string ident)
			{
				try
				{
					if (!nodeCstrMap.Contains(ident))
					{
						throw new IOException("No nodetype for " + ident);
					}
					return nodeCstrMap[ident].NewInstance(ident);
				}
				catch (MemberAccessException e)
				{
					throw new IOException(e);
				}
				catch (InstantiationException e)
				{
					throw new IOException(e);
				}
				catch (TargetInvocationException e)
				{
					throw new IOException(e);
				}
			}

			private static readonly Type[] ncstrSig = new Type[] { typeof(string) };

			private static readonly IDictionary<string, Constructor<Parser.Node>> nodeCstrMap
				 = new Dictionary<string, Constructor<Parser.Node>>();

			protected internal static readonly IDictionary<string, Constructor<ComposableRecordReader
				>> rrCstrMap = new Dictionary<string, Constructor<ComposableRecordReader>>();

			/// <summary>
			/// For a given identifier, add a mapping to the nodetype for the parse
			/// tree and to the ComposableRecordReader to be created, including the
			/// formals required to invoke the constructor.
			/// </summary>
			/// <remarks>
			/// For a given identifier, add a mapping to the nodetype for the parse
			/// tree and to the ComposableRecordReader to be created, including the
			/// formals required to invoke the constructor.
			/// The nodetype and constructor signature should be filled in from the
			/// child node.
			/// </remarks>
			/// <exception cref="System.MissingMethodException"/>
			protected internal static void AddIdentifier(string ident, Type[] mcstrSig, Type 
				nodetype, Type cl)
			{
				Constructor<Parser.Node> ncstr = nodetype.GetDeclaredConstructor(ncstrSig);
				nodeCstrMap[ident] = ncstr;
				Constructor<ComposableRecordReader> mcstr = cl.GetDeclaredConstructor(mcstrSig);
				rrCstrMap[ident] = mcstr;
			}

			protected internal int id = -1;

			protected internal string ident;

			protected internal Type cmpcl;

			protected internal Node(string ident)
			{
				// inst
				this.ident = ident;
			}

			protected internal virtual void SetID(int id)
			{
				this.id = id;
			}

			protected internal virtual void SetKeyComparator(Type cmpcl)
			{
				this.cmpcl = cmpcl;
			}

			/// <exception cref="System.IO.IOException"/>
			internal abstract void Parse(IList<Parser.Token> args, Configuration conf);
		}

		/// <summary>Nodetype in the parse tree for &quot;wrapped&quot; InputFormats.</summary>
		internal class WNode : Parser.Node
		{
			private static readonly Type[] cstrSig = new Type[] { typeof(int), typeof(RecordReader
				), typeof(Type) };

			/// <exception cref="System.MissingMethodException"/>
			internal static void AddIdentifier(string ident, Type cl)
			{
				Parser.Node.AddIdentifier(ident, cstrSig, typeof(Parser.WNode), cl);
			}

			private string indir;

			private InputFormat<object, object> inf;

			public WNode(string ident)
				: base(ident)
			{
			}

			/// <summary>
			/// Let the first actual define the InputFormat and the second define
			/// the <tt>mapred.input.dir</tt> property.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			internal override void Parse(IList<Parser.Token> ll, Configuration conf)
			{
				StringBuilder sb = new StringBuilder();
				IEnumerator<Parser.Token> i = ll.GetEnumerator();
				while (i.HasNext())
				{
					Parser.Token t = i.Next();
					if (Parser.TType.Comma.Equals(t.GetType()))
					{
						try
						{
							inf = (InputFormat<object, object>)ReflectionUtils.NewInstance(conf.GetClassByName
								(sb.ToString()), conf);
						}
						catch (TypeLoadException e)
						{
							throw new IOException(e);
						}
						catch (ArgumentException e)
						{
							throw new IOException(e);
						}
						break;
					}
					sb.Append(t.GetStr());
				}
				if (!i.HasNext())
				{
					throw new IOException("Parse error");
				}
				Parser.Token t_1 = i.Next();
				if (!Parser.TType.Quot.Equals(t_1.GetType()))
				{
					throw new IOException("Expected quoted string");
				}
				indir = t_1.GetStr();
			}

			// no check for ll.isEmpty() to permit extension
			/// <exception cref="System.IO.IOException"/>
			private Configuration GetConf(Configuration jconf)
			{
				Job job = Job.GetInstance(jconf);
				FileInputFormat.SetInputPaths(job, indir);
				return job.GetConfiguration();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override IList GetSplits(JobContext context)
			{
				return inf.GetSplits(new JobContextImpl(GetConf(context.GetConfiguration()), context
					.GetJobID()));
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override RecordReader CreateRecordReader(InputSplit split, TaskAttemptContext
				 taskContext)
			{
				try
				{
					if (!rrCstrMap.Contains(ident))
					{
						throw new IOException("No RecordReader for " + ident);
					}
					Configuration conf = GetConf(taskContext.GetConfiguration());
					TaskAttemptContext context = new TaskAttemptContextImpl(conf, TaskAttemptID.ForName
						(conf.Get(MRJobConfig.TaskAttemptId)), new Parser.WrappedStatusReporter(taskContext
						));
					return rrCstrMap[ident].NewInstance(id, inf.CreateRecordReader(split, context), cmpcl
						);
				}
				catch (MemberAccessException e)
				{
					throw new IOException(e);
				}
				catch (InstantiationException e)
				{
					throw new IOException(e);
				}
				catch (TargetInvocationException e)
				{
					throw new IOException(e);
				}
			}

			public override string ToString()
			{
				return ident + "(" + inf.GetType().FullName + ",\"" + indir + "\")";
			}
		}

		private class WrappedStatusReporter : StatusReporter
		{
			internal TaskAttemptContext context;

			public WrappedStatusReporter(TaskAttemptContext context)
			{
				this.context = context;
			}

			public override Counter GetCounter<_T0>(Enum<_T0> name)
			{
				return context.GetCounter(name);
			}

			public override Counter GetCounter(string group, string name)
			{
				return context.GetCounter(group, name);
			}

			public override void Progress()
			{
				context.Progress();
			}

			public override float GetProgress()
			{
				return context.GetProgress();
			}

			public override void SetStatus(string status)
			{
				context.SetStatus(status);
			}
		}

		/// <summary>Internal nodetype for &quot;composite&quot; InputFormats.</summary>
		internal class CNode : Parser.Node
		{
			private static readonly Type[] cstrSig = new Type[] { typeof(int), typeof(Configuration
				), typeof(int), typeof(Type) };

			/// <exception cref="System.MissingMethodException"/>
			internal static void AddIdentifier(string ident, Type cl)
			{
				Parser.Node.AddIdentifier(ident, cstrSig, typeof(Parser.CNode), cl);
			}

			private AList<Parser.Node> kids = new AList<Parser.Node>();

			public CNode(string ident)
				: base(ident)
			{
			}

			// inst
			protected internal override void SetKeyComparator(Type cmpcl)
			{
				base.SetKeyComparator(cmpcl);
				foreach (Parser.Node n in kids)
				{
					n.SetKeyComparator(cmpcl);
				}
			}

			/// <summary>
			/// Combine InputSplits from child InputFormats into a
			/// <see cref="CompositeInputSplit"/>
			/// .
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override IList GetSplits(JobContext job)
			{
				IList<IList<InputSplit>> splits = new AList<IList<InputSplit>>(kids.Count);
				for (int i = 0; i < kids.Count; ++i)
				{
					IList<InputSplit> tmp = kids[i].GetSplits(job);
					if (null == tmp)
					{
						throw new IOException("Error gathering splits from child RReader");
					}
					if (i > 0 && splits[i - 1].Count != tmp.Count)
					{
						throw new IOException("Inconsistent split cardinality from child " + i + " (" + splits
							[i - 1].Count + "/" + tmp.Count + ")");
					}
					splits.Add(i, tmp);
				}
				int size = splits[0].Count;
				IList<InputSplit> ret = new AList<InputSplit>();
				for (int i_1 = 0; i_1 < size; ++i_1)
				{
					CompositeInputSplit split = new CompositeInputSplit(splits.Count);
					for (int j = 0; j < splits.Count; ++j)
					{
						split.Add(splits[j][i_1]);
					}
					ret.AddItem(split);
				}
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override RecordReader CreateRecordReader(InputSplit split, TaskAttemptContext
				 taskContext)
			{
				// child types unknowable
				if (!(split is CompositeInputSplit))
				{
					throw new IOException("Invalid split type:" + split.GetType().FullName);
				}
				CompositeInputSplit spl = (CompositeInputSplit)split;
				int capacity = kids.Count;
				CompositeRecordReader ret = null;
				try
				{
					if (!rrCstrMap.Contains(ident))
					{
						throw new IOException("No RecordReader for " + ident);
					}
					ret = (CompositeRecordReader)rrCstrMap[ident].NewInstance(id, taskContext.GetConfiguration
						(), capacity, cmpcl);
				}
				catch (MemberAccessException e)
				{
					throw new IOException(e);
				}
				catch (InstantiationException e)
				{
					throw new IOException(e);
				}
				catch (TargetInvocationException e)
				{
					throw new IOException(e);
				}
				for (int i = 0; i < capacity; ++i)
				{
					ret.Add(((ComposableRecordReader)kids[i].CreateRecordReader(spl.Get(i), taskContext
						)));
				}
				return (ComposableRecordReader)ret;
			}

			/// <summary>Parse a list of comma-separated nodes.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal override void Parse(IList<Parser.Token> args, Configuration conf)
			{
				ListIterator<Parser.Token> i = args.ListIterator();
				while (i.HasNext())
				{
					Parser.Token t = i.Next();
					t.GetNode().SetID(i.PreviousIndex() >> 1);
					kids.AddItem(t.GetNode());
					if (i.HasNext() && !Parser.TType.Comma.Equals(i.Next().GetType()))
					{
						throw new IOException("Expected ','");
					}
				}
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				sb.Append(ident + "(");
				foreach (Parser.Node n in kids)
				{
					sb.Append(n.ToString() + ",");
				}
				Sharpen.Runtime.SetCharAt(sb, sb.Length - 1, ')');
				return sb.ToString();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static Parser.Token Reduce(Stack<Parser.Token> st, Configuration conf)
		{
			List<Parser.Token> args = new List<Parser.Token>();
			while (!st.IsEmpty() && !Parser.TType.Lparen.Equals(st.Peek().GetType()))
			{
				args.AddFirst(st.Pop());
			}
			if (st.IsEmpty())
			{
				throw new IOException("Unmatched ')'");
			}
			st.Pop();
			if (st.IsEmpty() || !Parser.TType.Ident.Equals(st.Peek().GetType()))
			{
				throw new IOException("Identifier expected");
			}
			Parser.Node n = Parser.Node.ForIdent(st.Pop().GetStr());
			n.Parse(args, conf);
			return new Parser.NodeToken(n);
		}

		/// <summary>
		/// Given an expression and an optional comparator, build a tree of
		/// InputFormats using the comparator to sort keys.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal static Parser.Node Parse(string expr, Configuration conf)
		{
			if (null == expr)
			{
				throw new IOException("Expression is null");
			}
			Type cmpcl = conf.GetClass<WritableComparator>(CompositeInputFormat.JoinComparator
				, null);
			Parser.Lexer lex = new Parser.Lexer(expr);
			Stack<Parser.Token> st = new Stack<Parser.Token>();
			Parser.Token tok;
			while ((tok = lex.Next()) != null)
			{
				if (Parser.TType.Rparen.Equals(tok.GetType()))
				{
					st.Push(Reduce(st, conf));
				}
				else
				{
					st.Push(tok);
				}
			}
			if (st.Count == 1 && Parser.TType.Cif.Equals(st.Peek().GetType()))
			{
				Parser.Node ret = st.Pop().GetNode();
				if (cmpcl != null)
				{
					ret.SetKeyComparator(cmpcl);
				}
				return ret;
			}
			throw new IOException("Missing ')'");
		}
	}
}
