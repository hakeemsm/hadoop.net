using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Collect;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Hadoop.Common.Core.Util;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token
{
	/// <summary>The client-side form of the token.</summary>
	public class Token<T> : IWritable
		where T : TokenIdentifier
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.Token.Token
			));

		private static IDictionary<Text, Type> tokenKindMap;

		private byte[] identifier;

		private byte[] password;

		private Text kind;

		private Text service;

		private TokenRenewer renewer;

		/// <summary>
		/// Construct a token given a token identifier and a secret manager for the
		/// type of the token identifier.
		/// </summary>
		/// <param name="id">the token identifier</param>
		/// <param name="mgr">the secret manager</param>
		public Token(T id, SecretManager<T> mgr)
		{
			password = mgr.CreatePassword(id);
			identifier = id.GetBytes();
			kind = id.GetKind();
			service = new Text();
		}

		/// <summary>Construct a token from the components.</summary>
		/// <param name="identifier">the token identifier</param>
		/// <param name="password">the token's password</param>
		/// <param name="kind">the kind of token</param>
		/// <param name="service">the service for this token</param>
		public Token(byte[] identifier, byte[] password, Text kind, Text service)
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
			kind = new Text();
			service = new Text();
		}

		/// <summary>Clone a token.</summary>
		/// <param name="other">the token to clone</param>
		public Token(Org.Apache.Hadoop.Security.Token.Token<T> other)
		{
			this.identifier = other.identifier;
			this.password = other.password;
			this.kind = other.kind;
			this.service = other.service;
		}

		/// <summary>Get the token identifier's byte representation</summary>
		/// <returns>the token identifier's byte representation</returns>
		public virtual byte[] GetIdentifier()
		{
			return identifier;
		}

		private static Type GetClassForIdentifier(Text kind)
		{
			Type cls = null;
			lock (typeof(Org.Apache.Hadoop.Security.Token.Token))
			{
				if (tokenKindMap == null)
				{
					tokenKindMap = Maps.NewHashMap();
					foreach (TokenIdentifier id in ServiceLoader.Load<TokenIdentifier>())
					{
						tokenKindMap[id.GetKind()] = id.GetType();
					}
				}
				cls = tokenKindMap[kind];
			}
			if (cls == null)
			{
				Log.Warn("Cannot find class for token kind " + kind);
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
		public virtual T DecodeIdentifier()
		{
			Type cls = GetClassForIdentifier(GetKind());
			if (cls == null)
			{
				return null;
			}
			TokenIdentifier tokenIdentifier = ReflectionUtils.NewInstance(cls, null);
			ByteArrayInputStream buf = new ByteArrayInputStream(identifier);
			DataInputStream @in = new DataInputStream(buf);
			tokenIdentifier.ReadFields(@in);
			@in.Close();
			return (T)tokenIdentifier;
		}

		/// <summary>Get the token password/secret</summary>
		/// <returns>the token password/secret</returns>
		public virtual byte[] GetPassword()
		{
			return password;
		}

		/// <summary>Get the token kind</summary>
		/// <returns>the kind of the token</returns>
		public virtual Text GetKind()
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
		[InterfaceAudience.Private]
		public virtual void SetKind(Text newKind)
		{
			lock (this)
			{
				kind = newKind;
				renewer = null;
			}
		}

		/// <summary>Get the service on which the token is supposed to be used</summary>
		/// <returns>the service name</returns>
		public virtual Text GetService()
		{
			return service;
		}

		/// <summary>Set the service on which the token is supposed to be used</summary>
		/// <param name="newService">the service name</param>
		public virtual void SetService(Text newService)
		{
			service = newService;
		}

		/// <summary>Indicates whether the token is a clone.</summary>
		/// <remarks>
		/// Indicates whether the token is a clone.  Used by HA failover proxy
		/// to indicate a token should not be visible to the user via
		/// UGI.getCredentials()
		/// </remarks>
		public class PrivateToken<T> : Org.Apache.Hadoop.Security.Token.Token<T>
			where T : TokenIdentifier
		{
			public PrivateToken(Org.Apache.Hadoop.Security.Token.Token<T> token)
				: base(token)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			int len = WritableUtils.ReadVInt(@in);
			if (identifier == null || identifier.Length != len)
			{
				identifier = new byte[len];
			}
			@in.ReadFully(identifier);
			len = WritableUtils.ReadVInt(@in);
			if (password == null || password.Length != len)
			{
				password = new byte[len];
			}
			@in.ReadFully(password);
			kind.ReadFields(@in);
			service.ReadFields(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			WritableUtils.WriteVInt(@out, identifier.Length);
			@out.Write(identifier);
			WritableUtils.WriteVInt(@out, password.Length);
			@out.Write(password);
			kind.Write(@out);
			service.Write(@out);
		}

		/// <summary>
		/// Generate a string with the url-quoted base64 encoded serialized form
		/// of the Writable.
		/// </summary>
		/// <param name="obj">the object to serialize</param>
		/// <returns>the encoded string</returns>
		/// <exception cref="System.IO.IOException"/>
		private static string EncodeWritable(IWritable obj)
		{
			DataOutputBuffer buf = new DataOutputBuffer();
			obj.Write(buf);
			Base64 encoder = new Base64(0, null, true);
			byte[] raw = new byte[buf.GetLength()];
			System.Array.Copy(buf.GetData(), 0, raw, 0, buf.GetLength());
			return encoder.EncodeToString(raw);
		}

		/// <summary>Modify the writable to the value from the newValue</summary>
		/// <param name="obj">the object to read into</param>
		/// <param name="newValue">the string with the url-safe base64 encoded bytes</param>
		/// <exception cref="System.IO.IOException"/>
		private static void DecodeWritable(IWritable obj, string newValue)
		{
			Base64 decoder = new Base64(0, null, true);
			DataInputBuffer buf = new DataInputBuffer();
			byte[] decoded = decoder.Decode(newValue);
			buf.Reset(decoded, decoded.Length);
			obj.ReadFields(buf);
		}

		/// <summary>Encode this token as a url safe string</summary>
		/// <returns>the encoded string</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual string EncodeToUrlString()
		{
			return EncodeWritable(this);
		}

		/// <summary>Decode the given url safe string into this token.</summary>
		/// <param name="newValue">the encoded string</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void DecodeFromUrlString(string newValue)
		{
			DecodeWritable(this, newValue);
		}

		public override bool Equals(object right)
		{
			if (this == right)
			{
				return true;
			}
			else
			{
				if (right == null || GetType() != right.GetType())
				{
					return false;
				}
				else
				{
					Org.Apache.Hadoop.Security.Token.Token<T> r = (Org.Apache.Hadoop.Security.Token.Token
						<T>)right;
					return Arrays.Equals(identifier, r.identifier) && Arrays.Equals(password, r.password
						) && kind.Equals(r.kind) && service.Equals(r.service);
				}
			}
		}

		public override int GetHashCode()
		{
			return WritableComparator.HashBytes(identifier, identifier.Length);
		}

		private static void AddBinaryBuffer(StringBuilder buffer, byte[] bytes)
		{
			for (int idx = 0; idx < bytes.Length; idx++)
			{
				// if not the first, put a blank separator in
				if (idx != 0)
				{
					buffer.Append(' ');
				}
				string num = Sharpen.Extensions.ToHexString(unchecked((int)(0xff)) & bytes[idx]);
				// if it is only one digit, add a leading 0.
				if (num.Length < 2)
				{
					buffer.Append('0');
				}
				buffer.Append(num);
			}
		}

		private void IdentifierToString(StringBuilder buffer)
		{
			T id = null;
			try
			{
				id = DecodeIdentifier();
			}
			catch (IOException)
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
					AddBinaryBuffer(buffer, identifier);
				}
			}
		}

		public override string ToString()
		{
			StringBuilder buffer = new StringBuilder();
			buffer.Append("Kind: ");
			buffer.Append(kind.ToString());
			buffer.Append(", Service: ");
			buffer.Append(service.ToString());
			buffer.Append(", Ident: ");
			IdentifierToString(buffer);
			return buffer.ToString();
		}

		private static ServiceLoader<TokenRenewer> renewers = ServiceLoader.Load<TokenRenewer
			>();

		/// <exception cref="System.IO.IOException"/>
		private TokenRenewer GetRenewer()
		{
			lock (this)
			{
				if (renewer != null)
				{
					return renewer;
				}
				renewer = TrivialRenewer;
				lock (renewers)
				{
					foreach (TokenRenewer canidate in renewers)
					{
						if (canidate.HandleKind(this.kind))
						{
							renewer = canidate;
							return renewer;
						}
					}
				}
				Log.Warn("No TokenRenewer defined for token kind " + this.kind);
				return renewer;
			}
		}

		/// <summary>Is this token managed so that it can be renewed or cancelled?</summary>
		/// <returns>true, if it can be renewed and cancelled.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsManaged()
		{
			return GetRenewer().IsManaged(this);
		}

		/// <summary>Renew this delegation token</summary>
		/// <returns>the new expiration time</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual long Renew(Configuration conf)
		{
			return GetRenewer().Renew(this, conf);
		}

		/// <summary>Cancel this delegation token</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Cancel(Configuration conf)
		{
			GetRenewer().Cancel(this, conf);
		}

		/// <summary>A trivial renewer for token kinds that aren't managed.</summary>
		/// <remarks>
		/// A trivial renewer for token kinds that aren't managed. Sub-classes need
		/// to implement getKind for their token kind.
		/// </remarks>
		public class TrivialRenewer : TokenRenewer
		{
			// define the kind for this renewer
			protected internal virtual Text GetKind()
			{
				return null;
			}

			public override bool HandleKind(Text kind)
			{
				return kind.Equals(GetKind());
			}

			public override bool IsManaged<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				)
			{
				return false;
			}

			public override long Renew<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				, Configuration conf)
			{
				throw new NotSupportedException("Token renewal is not supported " + " for " + token
					.kind + " tokens");
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Cancel<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				, Configuration conf)
			{
				throw new NotSupportedException("Token cancel is not supported " + " for " + token
					.kind + " tokens");
			}
		}

		private static readonly TokenRenewer TrivialRenewer = new Token.TrivialRenewer();
	}
}
