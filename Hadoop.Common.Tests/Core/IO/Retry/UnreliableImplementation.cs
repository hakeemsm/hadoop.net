using System.IO;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Retry
{
	internal class UnreliableImplementation : UnreliableInterface
	{
		private int failsOnceInvocationCount;

		private int failsOnceWithValueInvocationCount;

		private int failsTenTimesInvocationCount;

		private int succeedsOnceThenFailsCount;

		private int succeedsOnceThenFailsIdempotentCount;

		private int succeedsTenTimesThenFailsCount;

		private string identifier;

		private UnreliableImplementation.TypeOfExceptionToFailWith exceptionToFailWith;

		public enum TypeOfExceptionToFailWith
		{
			UnreliableException,
			StandbyException,
			IoException,
			RemoteException
		}

		public UnreliableImplementation()
			: this(null)
		{
		}

		public UnreliableImplementation(string identifier)
			: this(identifier, UnreliableImplementation.TypeOfExceptionToFailWith.UnreliableException
				)
		{
		}

		public virtual void SetIdentifier(string identifier)
		{
			this.identifier = identifier;
		}

		public UnreliableImplementation(string identifier, UnreliableImplementation.TypeOfExceptionToFailWith
			 exceptionToFailWith)
		{
			this.identifier = identifier;
			this.exceptionToFailWith = exceptionToFailWith;
		}

		public override void AlwaysSucceeds()
		{
		}

		// do nothing
		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.FatalException"/>
		public override void AlwaysFailsWithFatalException()
		{
			throw new UnreliableInterface.FatalException();
		}

		/// <exception cref="Org.Apache.Hadoop.Ipc.RemoteException"/>
		public override void AlwaysFailsWithRemoteFatalException()
		{
			throw new RemoteException(typeof(UnreliableInterface.FatalException).FullName, "Oops"
				);
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public override void FailsOnceThenSucceeds()
		{
			if (failsOnceInvocationCount++ == 0)
			{
				throw new UnreliableInterface.UnreliableException();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public override bool FailsOnceThenSucceedsWithReturnValue()
		{
			if (failsOnceWithValueInvocationCount++ == 0)
			{
				throw new UnreliableInterface.UnreliableException();
			}
			return true;
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public override void FailsTenTimesThenSucceeds()
		{
			if (failsTenTimesInvocationCount++ < 10)
			{
				throw new UnreliableInterface.UnreliableException();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		public override string SucceedsOnceThenFailsReturningString()
		{
			if (succeedsOnceThenFailsCount++ < 1)
			{
				return identifier;
			}
			else
			{
				ThrowAppropriateException(exceptionToFailWith, identifier);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		public override string SucceedsTenTimesThenFailsReturningString()
		{
			if (succeedsTenTimesThenFailsCount++ < 10)
			{
				return identifier;
			}
			else
			{
				ThrowAppropriateException(exceptionToFailWith, identifier);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public override string SucceedsOnceThenFailsReturningStringIdempotent()
		{
			if (succeedsOnceThenFailsIdempotentCount++ < 1)
			{
				return identifier;
			}
			else
			{
				ThrowAppropriateException(exceptionToFailWith, identifier);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public override string FailsIfIdentifierDoesntMatch(string identifier)
		{
			if (this.identifier.Equals(identifier))
			{
				return identifier;
			}
			else
			{
				string message = "expected '" + this.identifier + "' but received '" + identifier
					 + "'";
				ThrowAppropriateException(exceptionToFailWith, message);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void NonIdempotentVoidFailsIfIdentifierDoesntMatch(string identifier
			)
		{
			if (this.identifier.Equals(identifier))
			{
				return;
			}
			else
			{
				string message = "expected '" + this.identifier + "' but received '" + identifier
					 + "'";
				ThrowAppropriateException(exceptionToFailWith, message);
			}
		}

		public override string ToString()
		{
			return GetType().Name + "[" + identifier + "]";
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		private static void ThrowAppropriateException(UnreliableImplementation.TypeOfExceptionToFailWith
			 eType, string message)
		{
			switch (eType)
			{
				case UnreliableImplementation.TypeOfExceptionToFailWith.StandbyException:
				{
					throw new StandbyException(message);
				}

				case UnreliableImplementation.TypeOfExceptionToFailWith.UnreliableException:
				{
					throw new UnreliableInterface.UnreliableException(message);
				}

				case UnreliableImplementation.TypeOfExceptionToFailWith.IoException:
				{
					throw new IOException(message);
				}

				case UnreliableImplementation.TypeOfExceptionToFailWith.RemoteException:
				{
					throw new RemoteException(typeof(IOException).FullName, message);
				}

				default:
				{
					throw new RuntimeException(message);
				}
			}
		}
	}
}
