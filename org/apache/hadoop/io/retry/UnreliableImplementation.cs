using Sharpen;

namespace org.apache.hadoop.io.retry
{
	internal class UnreliableImplementation : org.apache.hadoop.io.retry.UnreliableInterface
	{
		private int failsOnceInvocationCount;

		private int failsOnceWithValueInvocationCount;

		private int failsTenTimesInvocationCount;

		private int succeedsOnceThenFailsCount;

		private int succeedsOnceThenFailsIdempotentCount;

		private int succeedsTenTimesThenFailsCount;

		private string identifier;

		private org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
			 exceptionToFailWith;

		public enum TypeOfExceptionToFailWith
		{
			UNRELIABLE_EXCEPTION,
			STANDBY_EXCEPTION,
			IO_EXCEPTION,
			REMOTE_EXCEPTION
		}

		public UnreliableImplementation()
			: this(null)
		{
		}

		public UnreliableImplementation(string identifier)
			: this(identifier, org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				.UNRELIABLE_EXCEPTION)
		{
		}

		public virtual void setIdentifier(string identifier)
		{
			this.identifier = identifier;
		}

		public UnreliableImplementation(string identifier, org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
			 exceptionToFailWith)
		{
			this.identifier = identifier;
			this.exceptionToFailWith = exceptionToFailWith;
		}

		public override void alwaysSucceeds()
		{
		}

		// do nothing
		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.FatalException"/>
		public override void alwaysFailsWithFatalException()
		{
			throw new org.apache.hadoop.io.retry.UnreliableInterface.FatalException();
		}

		/// <exception cref="org.apache.hadoop.ipc.RemoteException"/>
		public override void alwaysFailsWithRemoteFatalException()
		{
			throw new org.apache.hadoop.ipc.RemoteException(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.retry.UnreliableInterface.FatalException)).getName(), "Oops"
				);
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public override void failsOnceThenSucceeds()
		{
			if (failsOnceInvocationCount++ == 0)
			{
				throw new org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException();
			}
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public override bool failsOnceThenSucceedsWithReturnValue()
		{
			if (failsOnceWithValueInvocationCount++ == 0)
			{
				throw new org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException();
			}
			return true;
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public override void failsTenTimesThenSucceeds()
		{
			if (failsTenTimesInvocationCount++ < 10)
			{
				throw new org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException();
			}
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		public override string succeedsOnceThenFailsReturningString()
		{
			if (succeedsOnceThenFailsCount++ < 1)
			{
				return identifier;
			}
			else
			{
				throwAppropriateException(exceptionToFailWith, identifier);
				return null;
			}
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		public override string succeedsTenTimesThenFailsReturningString()
		{
			if (succeedsTenTimesThenFailsCount++ < 10)
			{
				return identifier;
			}
			else
			{
				throwAppropriateException(exceptionToFailWith, identifier);
				return null;
			}
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public override string succeedsOnceThenFailsReturningStringIdempotent()
		{
			if (succeedsOnceThenFailsIdempotentCount++ < 1)
			{
				return identifier;
			}
			else
			{
				throwAppropriateException(exceptionToFailWith, identifier);
				return null;
			}
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public override string failsIfIdentifierDoesntMatch(string identifier)
		{
			if (this.identifier.Equals(identifier))
			{
				return identifier;
			}
			else
			{
				string message = "expected '" + this.identifier + "' but received '" + identifier
					 + "'";
				throwAppropriateException(exceptionToFailWith, message);
				return null;
			}
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void nonIdempotentVoidFailsIfIdentifierDoesntMatch(string identifier
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
				throwAppropriateException(exceptionToFailWith, message);
			}
		}

		public override string ToString()
		{
			return Sharpen.Runtime.getClassForObject(this).getSimpleName() + "[" + identifier
				 + "]";
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		private static void throwAppropriateException(org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
			 eType, string message)
		{
			switch (eType)
			{
				case org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
					.STANDBY_EXCEPTION:
				{
					throw new org.apache.hadoop.ipc.StandbyException(message);
				}

				case org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
					.UNRELIABLE_EXCEPTION:
				{
					throw new org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException(message
						);
				}

				case org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
					.IO_EXCEPTION:
				{
					throw new System.IO.IOException(message);
				}

				case org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
					.REMOTE_EXCEPTION:
				{
					throw new org.apache.hadoop.ipc.RemoteException(Sharpen.Runtime.getClassForType(typeof(
						System.IO.IOException)).getName(), message);
				}

				default:
				{
					throw new System.Exception(message);
				}
			}
		}
	}
}
