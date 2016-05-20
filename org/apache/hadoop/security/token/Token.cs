using Sharpen;

namespace org.apache.hadoop.security.token
{
	/// <summary>The client-side form of the token.</summary>
	public class Token<T> : org.apache.hadoop.io.Writable
		where T : org.apache.hadoop.security.token.TokenIdentifier
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.token.Token
			)));

		private static System.Collections.Generic.IDictionary<org.apache.hadoop.io.Text, 
			java.lang.Class> tokenKindMap;

		private byte[] identifier;

		private byte[] password;

		private org.apache.hadoop.io.Text kind;

		private org.apache.hadoop.io.Text service;

		private org.apache.hadoop.security.token.TokenRenewer renewer;

		/// <summary>
		/// Construct a token given a token identifier and a secret manager for the
		/// type of the token identifier.
		/// </summary>
		/// <param name="id">the token identifier</param>
		/// <param name="mgr">the secret manager</param>
		public Token(T id, org.apache.hadoop.security.token.SecretManager<T> mgr)
		{
			password = mgr.createPassword(id);
			identifier = id.getBytes();
			kind = id.getKind();
			service = new org.apache.hadoop.io.Text();
		}

		/// <summary>Construct a token from the components.</summary>
		/// <param name="identifier">the token identifier</param>
		/// <param name="password">the token's password</param>
		/// <param name="kind">the kind of token</param>
		/// <param name="service">the service for this token</param>
		public Token(byte[] identifier, byte[] password, org.apache.hadoop.io.Text kind, 
			org.apache.hadoop.io.Text service)
		{
			this.identifier = identifier;
			this.password = password;
			this.kind = kind;
			this.service = service;
		}

		/// <summary>Default constructor</summary>
		public Token()
		{
			identifier = new byte[0];
			password = new byte[0];
			kind = new org.apache.hadoop.io.Text();
			service = new org.apache.hadoop.io.Text();
		}

		/// <summary>Clone a token.</summary>
		/// <param name="other">the token to clone</param>
		public Token(org.apache.hadoop.security.token.Token<T> other)
		{
			this.identifier = other.identifier;
			this.password = other.password;
			this.kind = other.kind;
			this.service = other.service;
		}

		/// <summary>Get the token identifier's byte representation</summary>
		/// <returns>the token identifier's byte representation</returns>
		public virtual byte[] getIdentifier()
		{
			return identifier;
		}

		private static java.lang.Class getClassForIdentifier(org.apache.hadoop.io.Text kind
			)
		{
			java.lang.Class cls = null;
			lock (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.token.Token
				)))
			{
				if (tokenKindMap == null)
				{
					tokenKindMap = com.google.common.collect.Maps.newHashMap();
					foreach (org.apache.hadoop.security.token.TokenIdentifier id in java.util.ServiceLoader
						.load<org.apache.hadoop.security.token.TokenIdentifier>())
					{
						tokenKindMap[id.getKind()] = Sharpen.Runtime.getClassForObject(id);
					}
				}
				cls = tokenKindMap[kind];
			}
			if (cls == null)
			{
				LOG.warn("Cannot find class for token kind " + kind);
				return null;
			}
			return cls;
		}

		/// <summary>
		/// Get the token identifier object, or null if it could not be constructed
		/// (because the class could not be loaded, for example).
		/// </summary>
		/// <returns>the token identifier, or null</returns>
		/// <exception cref="System.IO.IOException"></exception>
		public virtual T decodeIdentifier()
		{
			java.lang.Class cls = getClassForIdentifier(getKind());
			if (cls == null)
			{
				return null;
			}
			org.apache.hadoop.security.token.TokenIdentifier tokenIdentifier = org.apache.hadoop.util.ReflectionUtils
				.newInstance(cls, null);
			java.io.ByteArrayInputStream buf = new java.io.ByteArrayInputStream(identifier);
			java.io.DataInputStream @in = new java.io.DataInputStream(buf);
			tokenIdentifier.readFields(@in);
			@in.close();
			return (T)tokenIdentifier;
		}

		/// <summary>Get the token password/secret</summary>
		/// <returns>the token password/secret</returns>
		public virtual byte[] getPassword()
		{
			return password;
		}

		/// <summary>Get the token kind</summary>
		/// <returns>the kind of the token</returns>
		public virtual org.apache.hadoop.io.Text getKind()
		{
			lock (this)
			{
				return kind;
			}
		}

		/// <summary>Set the token kind.</summary>
		/// <remarks>
		/// Set the token kind. This is only intended to be used by services that
		/// wrap another service's token, such as HFTP wrapping HDFS.
		/// </remarks>
		/// <param name="newKind"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual void setKind(org.apache.hadoop.io.Text newKind)
		{
			lock (this)
			{
				kind = newKind;
				renewer = null;
			}
		}

		/// <summary>Get the service on which the token is supposed to be used</summary>
		/// <returns>the service name</returns>
		public virtual org.apache.hadoop.io.Text getService()
		{
			return service;
		}

		/// <summary>Set the service on which the token is supposed to be used</summary>
		/// <param name="newService">the service name</param>
		public virtual void setService(org.apache.hadoop.io.Text newService)
		{
			service = newService;
		}

		/// <summary>Indicates whether the token is a clone.</summary>
		/// <remarks>
		/// Indicates whether the token is a clone.  Used by HA failover proxy
		/// to indicate a token should not be visible to the user via
		/// UGI.getCredentials()
		/// </remarks>
		public class PrivateToken<T> : org.apache.hadoop.security.token.Token<T>
			where T : org.apache.hadoop.security.token.TokenIdentifier
		{
			public PrivateToken(org.apache.hadoop.security.token.Token<T> token)
				: base(token)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			int len = org.apache.hadoop.io.WritableUtils.readVInt(@in);
			if (identifier == null || identifier.Length != len)
			{
				identifier = new byte[len];
			}
			@in.readFully(identifier);
			len = org.apache.hadoop.io.WritableUtils.readVInt(@in);
			if (password == null || password.Length != len)
			{
				password = new byte[len];
			}
			@in.readFully(password);
			kind.readFields(@in);
			service.readFields(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, identifier.Length);
			@out.write(identifier);
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, password.Length);
			@out.write(password);
			kind.write(@out);
			service.write(@out);
		}

		/// <summary>
		/// Generate a string with the url-quoted base64 encoded serialized form
		/// of the Writable.
		/// </summary>
		/// <param name="obj">the object to serialize</param>
		/// <returns>the encoded string</returns>
		/// <exception cref="System.IO.IOException"/>
		private static string encodeWritable(org.apache.hadoop.io.Writable obj)
		{
			org.apache.hadoop.io.DataOutputBuffer buf = new org.apache.hadoop.io.DataOutputBuffer
				();
			obj.write(buf);
			org.apache.commons.codec.binary.Base64 encoder = new org.apache.commons.codec.binary.Base64
				(0, null, true);
			byte[] raw = new byte[buf.getLength()];
			System.Array.Copy(buf.getData(), 0, raw, 0, buf.getLength());
			return encoder.encodeToString(raw);
		}

		/// <summary>Modify the writable to the value from the newValue</summary>
		/// <param name="obj">the object to read into</param>
		/// <param name="newValue">the string with the url-safe base64 encoded bytes</param>
		/// <exception cref="System.IO.IOException"/>
		private static void decodeWritable(org.apache.hadoop.io.Writable obj, string newValue
			)
		{
			org.apache.commons.codec.binary.Base64 decoder = new org.apache.commons.codec.binary.Base64
				(0, null, true);
			org.apache.hadoop.io.DataInputBuffer buf = new org.apache.hadoop.io.DataInputBuffer
				();
			byte[] decoded = decoder.decode(newValue);
			buf.reset(decoded, decoded.Length);
			obj.readFields(buf);
		}

		/// <summary>Encode this token as a url safe string</summary>
		/// <returns>the encoded string</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual string encodeToUrlString()
		{
			return encodeWritable(this);
		}

		/// <summary>Decode the given url safe string into this token.</summary>
		/// <param name="newValue">the encoded string</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void decodeFromUrlString(string newValue)
		{
			decodeWritable(this, newValue);
		}

		public override bool Equals(object right)
		{
			if (this == right)
			{
				return true;
			}
			else
			{
				if (right == null || Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject
					(right))
				{
					return false;
				}
				else
				{
					org.apache.hadoop.security.token.Token<T> r = (org.apache.hadoop.security.token.Token
						<T>)right;
					return java.util.Arrays.equals(identifier, r.identifier) && java.util.Arrays.equals
						(password, r.password) && kind.Equals(r.kind) && service.Equals(r.service);
				}
			}
		}

		public override int GetHashCode()
		{
			return org.apache.hadoop.io.WritableComparator.hashBytes(identifier, identifier.Length
				);
		}

		private static void addBinaryBuffer(java.lang.StringBuilder buffer, byte[] bytes)
		{
			for (int idx = 0; idx < bytes.Length; idx++)
			{
				// if not the first, put a blank separator in
				if (idx != 0)
				{
					buffer.Append(' ');
				}
				string num = int.toHexString(unchecked((int)(0xff)) & bytes[idx]);
				// if it is only one digit, add a leading 0.
				if (num.Length < 2)
				{
					buffer.Append('0');
				}
				buffer.Append(num);
			}
		}

		private void identifierToString(java.lang.StringBuilder buffer)
		{
			T id = null;
			try
			{
				id = decodeIdentifier();
			}
			catch (System.IO.IOException)
			{
			}
			finally
			{
				// handle in the finally block
				if (id != null)
				{
					buffer.Append("(").Append(id).Append(")");
				}
				else
				{
					addBinaryBuffer(buffer, identifier);
				}
			}
		}

		public override string ToString()
		{
			java.lang.StringBuilder buffer = new java.lang.StringBuilder();
			buffer.Append("Kind: ");
			buffer.Append(kind.ToString());
			buffer.Append(", Service: ");
			buffer.Append(service.ToString());
			buffer.Append(", Ident: ");
			identifierToString(buffer);
			return buffer.ToString();
		}

		private static java.util.ServiceLoader<org.apache.hadoop.security.token.TokenRenewer
			> renewers = java.util.ServiceLoader.load<org.apache.hadoop.security.token.TokenRenewer
			>();

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.security.token.TokenRenewer getRenewer()
		{
			lock (this)
			{
				if (renewer != null)
				{
					return renewer;
				}
				renewer = TRIVIAL_RENEWER;
				lock (renewers)
				{
					foreach (org.apache.hadoop.security.token.TokenRenewer canidate in renewers)
					{
						if (canidate.handleKind(this.kind))
						{
							renewer = canidate;
							return renewer;
						}
					}
				}
				LOG.warn("No TokenRenewer defined for token kind " + this.kind);
				return renewer;
			}
		}

		/// <summary>Is this token managed so that it can be renewed or cancelled?</summary>
		/// <returns>true, if it can be renewed and cancelled.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool isManaged()
		{
			return getRenewer().isManaged(this);
		}

		/// <summary>Renew this delegation token</summary>
		/// <returns>the new expiration time</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual long renew(org.apache.hadoop.conf.Configuration conf)
		{
			return getRenewer().renew(this, conf);
		}

		/// <summary>Cancel this delegation token</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void cancel(org.apache.hadoop.conf.Configuration conf)
		{
			getRenewer().cancel(this, conf);
		}

		/// <summary>A trivial renewer for token kinds that aren't managed.</summary>
		/// <remarks>
		/// A trivial renewer for token kinds that aren't managed. Sub-classes need
		/// to implement getKind for their token kind.
		/// </remarks>
		public class TrivialRenewer : org.apache.hadoop.security.token.TokenRenewer
		{
			// define the kind for this renewer
			protected internal virtual org.apache.hadoop.io.Text getKind()
			{
				return null;
			}

			public override bool handleKind(org.apache.hadoop.io.Text kind)
			{
				return kind.Equals(getKind());
			}

			public override bool isManaged<_T0>(org.apache.hadoop.security.token.Token<_T0> token
				)
			{
				return false;
			}

			public override long renew<_T0>(org.apache.hadoop.security.token.Token<_T0> token
				, org.apache.hadoop.conf.Configuration conf)
			{
				throw new System.NotSupportedException("Token renewal is not supported " + " for "
					 + token.kind + " tokens");
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void cancel<_T0>(org.apache.hadoop.security.token.Token<_T0> token
				, org.apache.hadoop.conf.Configuration conf)
			{
				throw new System.NotSupportedException("Token cancel is not supported " + " for "
					 + token.kind + " tokens");
			}
		}

		private static readonly org.apache.hadoop.security.token.TokenRenewer TRIVIAL_RENEWER
			 = new org.apache.hadoop.security.token.Token.TrivialRenewer();
	}
}
