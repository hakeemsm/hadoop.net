/*
* $HeadURL$
* $Revision$
* $Date$
*
* ====================================================================
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
* ====================================================================
*
* This software consists of voluntary contributions made by many
* individuals on behalf of the Apache Software Foundation.  For more
* information on the Apache Software Foundation, please see
* <http://www.apache.org/>.
*
*/
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security.Ssl
{
	/// <summary>
	/// Copied from the not-yet-commons-ssl project at
	/// http://juliusdavies.ca/commons-ssl/
	/// This project is not yet in Apache, but it is Apache 2.0 licensed.
	/// </summary>
	/// <remarks>
	/// Copied from the not-yet-commons-ssl project at
	/// http://juliusdavies.ca/commons-ssl/
	/// This project is not yet in Apache, but it is Apache 2.0 licensed.
	/// Interface for checking if a hostname matches the names stored inside the
	/// server's X.509 certificate.  Correctly implements
	/// javax.net.ssl.HostnameVerifier, but that interface is not recommended.
	/// Instead we added several check() methods that take SSLSocket,
	/// or X509Certificate, or ultimately (they all end up calling this one),
	/// String.  (It's easier to supply JUnit with Strings instead of mock
	/// SSLSession objects!)
	/// </p><p>Our check() methods throw exceptions if the name is
	/// invalid, whereas javax.net.ssl.HostnameVerifier just returns true/false.
	/// <p/>
	/// We provide the HostnameVerifier.DEFAULT, HostnameVerifier.STRICT, and
	/// HostnameVerifier.ALLOW_ALL implementations.  We also provide the more
	/// specialized HostnameVerifier.DEFAULT_AND_LOCALHOST, as well as
	/// HostnameVerifier.STRICT_IE6.  But feel free to define your own
	/// implementations!
	/// <p/>
	/// Inspired by Sebastian Hauer's original StrictSSLProtocolSocketFactory in the
	/// HttpClient "contrib" repository.
	/// </remarks>
	public abstract class SSLHostnameVerifier : HostnameVerifier
	{
		public abstract bool Verify(string host, SSLSession session);

		/// <exception cref="System.IO.IOException"/>
		public abstract void Check(string host, SSLSocket ssl);

		/// <exception cref="SSLException"/>
		public abstract void Check(string host, X509Certificate cert);

		/// <exception cref="SSLException"/>
		public abstract void Check(string host, string[] cns, string[] subjectAlts);

		/// <exception cref="System.IO.IOException"/>
		public abstract void Check(string[] hosts, SSLSocket ssl);

		/// <exception cref="SSLException"/>
		public abstract void Check(string[] hosts, X509Certificate cert);

		/// <summary>
		/// Checks to see if the supplied hostname matches any of the supplied CNs
		/// or "DNS" Subject-Alts.
		/// </summary>
		/// <remarks>
		/// Checks to see if the supplied hostname matches any of the supplied CNs
		/// or "DNS" Subject-Alts.  Most implementations only look at the first CN,
		/// and ignore any additional CNs.  Most implementations do look at all of
		/// the "DNS" Subject-Alts. The CNs or Subject-Alts may contain wildcards
		/// according to RFC 2818.
		/// </remarks>
		/// <param name="cns">
		/// CN fields, in order, as extracted from the X.509
		/// certificate.
		/// </param>
		/// <param name="subjectAlts">
		/// Subject-Alt fields of type 2 ("DNS"), as extracted
		/// from the X.509 certificate.
		/// </param>
		/// <param name="hosts">The array of hostnames to verify.</param>
		/// <exception cref="SSLException">If verification failed.</exception>
		public abstract void Check(string[] hosts, string[] cns, string[] subjectAlts);

		private sealed class _AbstractVerifier_130 : SSLHostnameVerifier.AbstractVerifier
		{
			public _AbstractVerifier_130()
			{
			}

			/// <exception cref="SSLException"/>
			public sealed override void Check(string[] hosts, string[] cns, string[] subjectAlts
				)
			{
				this.Check(hosts, cns, subjectAlts, false, false);
			}

			public sealed override string ToString()
			{
				return "DEFAULT";
			}
		}

		/// <summary>The DEFAULT HostnameVerifier works the same way as Curl and Firefox.</summary>
		/// <remarks>
		/// The DEFAULT HostnameVerifier works the same way as Curl and Firefox.
		/// <p/>
		/// The hostname must match either the first CN, or any of the subject-alts.
		/// A wildcard can occur in the CN, and in any of the subject-alts.
		/// <p/>
		/// The only difference between DEFAULT and STRICT is that a wildcard (such
		/// as "*.foo.com") with DEFAULT matches all subdomains, including
		/// "a.b.foo.com".
		/// </remarks>
		public const SSLHostnameVerifier Default = new _AbstractVerifier_130();

		private sealed class _AbstractVerifier_150 : SSLHostnameVerifier.AbstractVerifier
		{
			public _AbstractVerifier_150()
			{
			}

			/// <exception cref="SSLException"/>
			public sealed override void Check(string[] hosts, string[] cns, string[] subjectAlts
				)
			{
				if (SSLHostnameVerifier.AbstractVerifier.IsLocalhost(hosts[0]))
				{
					return;
				}
				this.Check(hosts, cns, subjectAlts, false, false);
			}

			public sealed override string ToString()
			{
				return "DEFAULT_AND_LOCALHOST";
			}
		}

		/// <summary>
		/// The DEFAULT_AND_LOCALHOST HostnameVerifier works like the DEFAULT
		/// one with one additional relaxation:  a host of "localhost",
		/// "localhost.localdomain", "127.0.0.1", "::1" will always pass, no matter
		/// what is in the server's certificate.
		/// </summary>
		public const SSLHostnameVerifier DefaultAndLocalhost = new _AbstractVerifier_150(
			);

		private sealed class _AbstractVerifier_182 : SSLHostnameVerifier.AbstractVerifier
		{
			public _AbstractVerifier_182()
			{
			}

			/// <exception cref="SSLException"/>
			public sealed override void Check(string[] host, string[] cns, string[] subjectAlts
				)
			{
				this.Check(host, cns, subjectAlts, false, true);
			}

			public sealed override string ToString()
			{
				return "STRICT";
			}
		}

		/// <summary>
		/// The STRICT HostnameVerifier works the same way as java.net.URL in Sun
		/// Java 1.4, Sun Java 5, Sun Java 6.
		/// </summary>
		/// <remarks>
		/// The STRICT HostnameVerifier works the same way as java.net.URL in Sun
		/// Java 1.4, Sun Java 5, Sun Java 6.  It's also pretty close to IE6.
		/// This implementation appears to be compliant with RFC 2818 for dealing
		/// with wildcards.
		/// <p/>
		/// The hostname must match either the first CN, or any of the subject-alts.
		/// A wildcard can occur in the CN, and in any of the subject-alts.  The
		/// one divergence from IE6 is how we only check the first CN.  IE6 allows
		/// a match against any of the CNs present.  We decided to follow in
		/// Sun Java 1.4's footsteps and only check the first CN.
		/// <p/>
		/// A wildcard such as "*.foo.com" matches only subdomains in the same
		/// level, for example "a.foo.com".  It does not match deeper subdomains
		/// such as "a.b.foo.com".
		/// </remarks>
		public const SSLHostnameVerifier Strict = new _AbstractVerifier_182();

		private sealed class _AbstractVerifier_201 : SSLHostnameVerifier.AbstractVerifier
		{
			public _AbstractVerifier_201()
			{
			}

			/// <exception cref="SSLException"/>
			public sealed override void Check(string[] host, string[] cns, string[] subjectAlts
				)
			{
				this.Check(host, cns, subjectAlts, true, true);
			}

			public sealed override string ToString()
			{
				return "STRICT_IE6";
			}
		}

		/// <summary>
		/// The STRICT_IE6 HostnameVerifier works just like the STRICT one with one
		/// minor variation:  the hostname can match against any of the CN's in the
		/// server's certificate, not just the first one.
		/// </summary>
		/// <remarks>
		/// The STRICT_IE6 HostnameVerifier works just like the STRICT one with one
		/// minor variation:  the hostname can match against any of the CN's in the
		/// server's certificate, not just the first one.  This behaviour is
		/// identical to IE6's behaviour.
		/// </remarks>
		public const SSLHostnameVerifier StrictIe6 = new _AbstractVerifier_201();

		private sealed class _AbstractVerifier_218 : SSLHostnameVerifier.AbstractVerifier
		{
			public _AbstractVerifier_218()
			{
			}

			public sealed override void Check(string[] host, string[] cns, string[] subjectAlts
				)
			{
			}

			// Allow everything - so never blowup.
			public sealed override string ToString()
			{
				return "ALLOW_ALL";
			}
		}

		/// <summary>
		/// The ALLOW_ALL HostnameVerifier essentially turns hostname verification
		/// off.
		/// </summary>
		/// <remarks>
		/// The ALLOW_ALL HostnameVerifier essentially turns hostname verification
		/// off.  This implementation is a no-op, and never throws the SSLException.
		/// </remarks>
		public const SSLHostnameVerifier AllowAll = new _AbstractVerifier_218();

		public abstract class AbstractVerifier : SSLHostnameVerifier
		{
			/// <summary>
			/// This contains a list of 2nd-level domains that aren't allowed to
			/// have wildcards when combined with country-codes.
			/// </summary>
			/// <remarks>
			/// This contains a list of 2nd-level domains that aren't allowed to
			/// have wildcards when combined with country-codes.
			/// For example: [*.co.uk].
			/// <p/>
			/// The [*.co.uk] problem is an interesting one.  Should we just hope
			/// that CA's would never foolishly allow such a certificate to happen?
			/// Looks like we're the only implementation guarding against this.
			/// Firefox, Curl, Sun Java 1.4, 5, 6 don't bother with this check.
			/// </remarks>
			private static readonly string[] BadCountry2lds = new string[] { "ac", "co", "com"
				, "ed", "edu", "go", "gouv", "gov", "info", "lg", "ne", "net", "or", "org" };

			private static readonly string[] Localhosts = new string[] { "::1", "127.0.0.1", 
				"localhost", "localhost.localdomain" };

			static AbstractVerifier()
			{
				// Just in case developer forgot to manually sort the array.  :-)
				Arrays.Sort(BadCountry2lds);
				Arrays.Sort(Localhosts);
			}

			protected internal AbstractVerifier()
			{
			}

			/// <summary>The javax.net.ssl.HostnameVerifier contract.</summary>
			/// <param name="host">'hostname' we used to create our socket</param>
			/// <param name="session">SSLSession with the remote server</param>
			/// <returns>true if the host matched the one in the certificate.</returns>
			public override bool Verify(string host, SSLSession session)
			{
				try
				{
					Certificate[] certs = session.GetPeerCertificates();
					X509Certificate x509 = (X509Certificate)certs[0];
					Check(new string[] { host }, x509);
					return true;
				}
				catch (SSLException)
				{
					return false;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Check(string host, SSLSocket ssl)
			{
				Check(new string[] { host }, ssl);
			}

			/// <exception cref="SSLException"/>
			public override void Check(string host, X509Certificate cert)
			{
				Check(new string[] { host }, cert);
			}

			/// <exception cref="SSLException"/>
			public override void Check(string host, string[] cns, string[] subjectAlts)
			{
				Check(new string[] { host }, cns, subjectAlts);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Check(string[] host, SSLSocket ssl)
			{
				if (host == null)
				{
					throw new ArgumentNullException("host to verify is null");
				}
				SSLSession session = ssl.GetSession();
				if (session == null)
				{
					// In our experience this only happens under IBM 1.4.x when
					// spurious (unrelated) certificates show up in the server'
					// chain.  Hopefully this will unearth the real problem:
					InputStream @in = ssl.GetInputStream();
					@in.Available();
					/*
					If you're looking at the 2 lines of code above because
					you're running into a problem, you probably have two
					options:
					
					#1.  Clean up the certificate chain that your server
					is presenting (e.g. edit "/etc/apache2/server.crt"
					or wherever it is your server's certificate chain
					is defined).
					
					OR
					
					#2.   Upgrade to an IBM 1.5.x or greater JVM, or switch
					to a non-IBM JVM.
					*/
					// If ssl.getInputStream().available() didn't cause an
					// exception, maybe at least now the session is available?
					session = ssl.GetSession();
					if (session == null)
					{
						// If it's still null, probably a startHandshake() will
						// unearth the real problem.
						ssl.StartHandshake();
						// Okay, if we still haven't managed to cause an exception,
						// might as well go for the NPE.  Or maybe we're okay now?
						session = ssl.GetSession();
					}
				}
				Certificate[] certs;
				try
				{
					certs = session.GetPeerCertificates();
				}
				catch (SSLPeerUnverifiedException spue)
				{
					InputStream @in = ssl.GetInputStream();
					@in.Available();
					// Didn't trigger anything interesting?  Okay, just throw
					// original.
					throw;
				}
				X509Certificate x509 = (X509Certificate)certs[0];
				Check(host, x509);
			}

			/// <exception cref="SSLException"/>
			public override void Check(string[] host, X509Certificate cert)
			{
				string[] cns = SSLHostnameVerifier.Certificates.GetCNs(cert);
				string[] subjectAlts = SSLHostnameVerifier.Certificates.GetDNSSubjectAlts(cert);
				Check(host, cns, subjectAlts);
			}

			/// <exception cref="SSLException"/>
			public virtual void Check(string[] hosts, string[] cns, string[] subjectAlts, bool
				 ie6, bool strictWithSubDomains)
			{
				// Build up lists of allowed hosts For logging/debugging purposes.
				StringBuilder buf = new StringBuilder(32);
				buf.Append('<');
				for (int i = 0; i < hosts.Length; i++)
				{
					string h = hosts[i];
					h = h != null ? StringUtils.ToLowerCase(h.Trim()) : string.Empty;
					hosts[i] = h;
					if (i > 0)
					{
						buf.Append('/');
					}
					buf.Append(h);
				}
				buf.Append('>');
				string hostnames = buf.ToString();
				// Build the list of names we're going to check.  Our DEFAULT and
				// STRICT implementations of the HostnameVerifier only use the
				// first CN provided.  All other CNs are ignored.
				// (Firefox, wget, curl, Sun Java 1.4, 5, 6 all work this way).
				ICollection<string> names = new TreeSet<string>();
				if (cns != null && cns.Length > 0 && cns[0] != null)
				{
					names.AddItem(cns[0]);
					if (ie6)
					{
						for (int i_1 = 1; i_1 < cns.Length; i_1++)
						{
							names.AddItem(cns[i_1]);
						}
					}
				}
				if (subjectAlts != null)
				{
					for (int i_1 = 0; i_1 < subjectAlts.Length; i_1++)
					{
						if (subjectAlts[i_1] != null)
						{
							names.AddItem(subjectAlts[i_1]);
						}
					}
				}
				if (names.IsEmpty())
				{
					string msg = "Certificate for " + hosts[0] + " doesn't contain CN or DNS subjectAlt";
					throw new SSLException(msg);
				}
				// StringBuffer for building the error message.
				buf = new StringBuilder();
				bool match = false;
				for (IEnumerator<string> it = names.GetEnumerator(); it.HasNext(); )
				{
					// Don't trim the CN, though!
					string cn = StringUtils.ToLowerCase(it.Next());
					// Store CN in StringBuffer in case we need to report an error.
					buf.Append(" <");
					buf.Append(cn);
					buf.Append('>');
					if (it.HasNext())
					{
						buf.Append(" OR");
					}
					// The CN better have at least two dots if it wants wildcard
					// action.  It also can't be [*.co.uk] or [*.co.jp] or
					// [*.org.uk], etc...
					bool doWildcard = cn.StartsWith("*.") && cn.LastIndexOf('.') >= 0 && !IsIP4Address
						(cn) && AcceptableCountryWildcard(cn);
					for (int i_1 = 0; i_1 < hosts.Length; i_1++)
					{
						string hostName = StringUtils.ToLowerCase(hosts[i_1].Trim());
						if (doWildcard)
						{
							match = hostName.EndsWith(Runtime.Substring(cn, 1));
							if (match && strictWithSubDomains)
							{
								// If we're in strict mode, then [*.foo.com] is not
								// allowed to match [a.b.foo.com]
								match = CountDots(hostName) == CountDots(cn);
							}
						}
						else
						{
							match = hostName.Equals(cn);
						}
						if (match)
						{
							goto out_break;
						}
					}
out_continue: ;
				}
out_break: ;
				if (!match)
				{
					throw new SSLException("hostname in certificate didn't match: " + hostnames + " !="
						 + buf);
				}
			}

			public static bool IsIP4Address(string cn)
			{
				bool isIP4 = true;
				string tld = cn;
				int x = cn.LastIndexOf('.');
				// We only bother analyzing the characters after the final dot
				// in the name.
				if (x >= 0 && x + 1 < cn.Length)
				{
					tld = Runtime.Substring(cn, x + 1);
				}
				for (int i = 0; i < tld.Length; i++)
				{
					if (!char.IsDigit(tld[0]))
					{
						isIP4 = false;
						break;
					}
				}
				return isIP4;
			}

			public static bool AcceptableCountryWildcard(string cn)
			{
				int cnLen = cn.Length;
				if (cnLen >= 7 && cnLen <= 9)
				{
					// Look for the '.' in the 3rd-last position:
					if (cn[cnLen - 3] == '.')
					{
						// Trim off the [*.] and the [.XX].
						string s = Runtime.Substring(cn, 2, cnLen - 3);
						// And test against the sorted array of bad 2lds:
						int x = System.Array.BinarySearch(BadCountry2lds, s);
						return x < 0;
					}
				}
				return true;
			}

			public static bool IsLocalhost(string host)
			{
				host = host != null ? StringUtils.ToLowerCase(host.Trim()) : string.Empty;
				if (host.StartsWith("::1"))
				{
					int x = host.LastIndexOf('%');
					if (x >= 0)
					{
						host = Runtime.Substring(host, 0, x);
					}
				}
				int x_1 = System.Array.BinarySearch(Localhosts, host);
				return x_1 >= 0;
			}

			/// <summary>Counts the number of dots "." in a string.</summary>
			/// <param name="s">string to count dots from</param>
			/// <returns>number of dots</returns>
			public static int CountDots(string s)
			{
				int count = 0;
				for (int i = 0; i < s.Length; i++)
				{
					if (s[i] == '.')
					{
						count++;
					}
				}
				return count;
			}

			public abstract void Check(string[] arg1, string[] arg2, string[] arg3);
		}

		public class Certificates
		{
			public static string[] GetCNs(X509Certificate cert)
			{
				IList<string> cnList = new List<string>();
				/*
				Sebastian Hauer's original StrictSSLProtocolSocketFactory used
				getName() and had the following comment:
				
				Parses a X.500 distinguished name for the value of the
				"Common Name" field.  This is done a bit sloppy right
				now and should probably be done a bit more according to
				<code>RFC 2253</code>.
				
				I've noticed that toString() seems to do a better job than
				getName() on these X500Principal objects, so I'm hoping that
				addresses Sebastian's concern.
				
				For example, getName() gives me this:
				1.2.840.113549.1.9.1=#16166a756c6975736461766965734063756362632e636f6d
				
				whereas toString() gives me this:
				EMAILADDRESS=juliusdavies@cucbc.com
				
				Looks like toString() even works with non-ascii domain names!
				I tested it with "&#x82b1;&#x5b50;.co.jp" and it worked fine.
				*/
				string subjectPrincipal = cert.GetSubjectX500Principal().ToString();
				StringTokenizer st = new StringTokenizer(subjectPrincipal, ",");
				while (st.HasMoreTokens())
				{
					string tok = st.NextToken();
					int x = tok.IndexOf("CN=");
					if (x >= 0)
					{
						cnList.AddItem(Runtime.Substring(tok, x + 3));
					}
				}
				if (!cnList.IsEmpty())
				{
					string[] cns = new string[cnList.Count];
					Collections.ToArray(cnList, cns);
					return cns;
				}
				else
				{
					return null;
				}
			}

			/// <summary>Extracts the array of SubjectAlt DNS names from an X509Certificate.</summary>
			/// <remarks>
			/// Extracts the array of SubjectAlt DNS names from an X509Certificate.
			/// Returns null if there aren't any.
			/// <p/>
			/// Note:  Java doesn't appear able to extract international characters
			/// from the SubjectAlts.  It can only extract international characters
			/// from the CN field.
			/// <p/>
			/// (Or maybe the version of OpenSSL I'm using to test isn't storing the
			/// international characters correctly in the SubjectAlts?).
			/// </remarks>
			/// <param name="cert">X509Certificate</param>
			/// <returns>Array of SubjectALT DNS names stored in the certificate.</returns>
			public static string[] GetDNSSubjectAlts(X509Certificate cert)
			{
				IList<string> subjectAltList = new List<string>();
				ICollection<IList<object>> c = null;
				try
				{
					c = cert.GetSubjectAlternativeNames();
				}
				catch (CertificateParsingException cpe)
				{
					// Should probably log.debug() this?
					Runtime.PrintStackTrace(cpe);
				}
				if (c != null)
				{
					IEnumerator<IList<object>> it = c.GetEnumerator();
					while (it.HasNext())
					{
						IList<object> list = it.Next();
						int type = ((int)list[0]);
						// If type is 2, then we've got a dNSName
						if (type == 2)
						{
							string s = (string)list[1];
							subjectAltList.AddItem(s);
						}
					}
				}
				if (!subjectAltList.IsEmpty())
				{
					string[] subjectAlts = new string[subjectAltList.Count];
					Collections.ToArray(subjectAltList, subjectAlts);
					return subjectAlts;
				}
				else
				{
					return null;
				}
			}
		}
	}

	public static class SSLHostnameVerifierConstants
	{
	}
}
