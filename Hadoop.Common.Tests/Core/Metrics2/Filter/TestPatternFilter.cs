using System.Collections.Generic;
using Org.Apache.Commons.Configuration;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Filter
{
	public class TestPatternFilter
	{
		/// <summary>Filters should default to accept</summary>
		[Fact]
		public virtual void EmptyConfigShouldAccept()
		{
			SubsetConfiguration empty = new ConfigBuilder().Subset(string.Empty);
			ShouldAccept(empty, "anything");
			ShouldAccept(empty, Arrays.AsList(Interns.Tag("key", "desc", "value")));
			ShouldAccept(empty, MockMetricsRecord("anything", Arrays.AsList(Interns.Tag("key"
				, "desc", "value"))));
		}

		/// <summary>Filters should handle white-listing correctly</summary>
		[Fact]
		public virtual void IncludeOnlyShouldOnlyIncludeMatched()
		{
			SubsetConfiguration wl = new ConfigBuilder().Add("p.include", "foo").Add("p.include.tags"
				, "foo:f").Subset("p");
			ShouldAccept(wl, "foo");
			ShouldAccept(wl, Arrays.AsList(Interns.Tag("bar", string.Empty, string.Empty), Interns.Tag
				("foo", string.Empty, "f")), new bool[] { false, true });
			ShouldAccept(wl, MockMetricsRecord("foo", Arrays.AsList(Interns.Tag("bar", string.Empty
				, string.Empty), Interns.Tag("foo", string.Empty, "f"))));
			ShouldReject(wl, "bar");
			ShouldReject(wl, Arrays.AsList(Interns.Tag("bar", string.Empty, string.Empty)));
			ShouldReject(wl, Arrays.AsList(Interns.Tag("foo", string.Empty, "boo")));
			ShouldReject(wl, MockMetricsRecord("bar", Arrays.AsList(Interns.Tag("foo", string.Empty
				, "f"))));
			ShouldReject(wl, MockMetricsRecord("foo", Arrays.AsList(Interns.Tag("bar", string.Empty
				, string.Empty))));
		}

		/// <summary>Filters should handle black-listing correctly</summary>
		[Fact]
		public virtual void ExcludeOnlyShouldOnlyExcludeMatched()
		{
			SubsetConfiguration bl = new ConfigBuilder().Add("p.exclude", "foo").Add("p.exclude.tags"
				, "foo:f").Subset("p");
			ShouldAccept(bl, "bar");
			ShouldAccept(bl, Arrays.AsList(Interns.Tag("bar", string.Empty, string.Empty)));
			ShouldAccept(bl, MockMetricsRecord("bar", Arrays.AsList(Interns.Tag("bar", string.Empty
				, string.Empty))));
			ShouldReject(bl, "foo");
			ShouldReject(bl, Arrays.AsList(Interns.Tag("bar", string.Empty, string.Empty), Interns.Tag
				("foo", string.Empty, "f")), new bool[] { true, false });
			ShouldReject(bl, MockMetricsRecord("foo", Arrays.AsList(Interns.Tag("bar", string.Empty
				, string.Empty))));
			ShouldReject(bl, MockMetricsRecord("bar", Arrays.AsList(Interns.Tag("bar", string.Empty
				, string.Empty), Interns.Tag("foo", string.Empty, "f"))));
		}

		/// <summary>
		/// Filters should accepts unmatched item when both include and
		/// exclude patterns are present.
		/// </summary>
		[Fact]
		public virtual void ShouldAcceptUnmatchedWhenBothAreConfigured()
		{
			SubsetConfiguration c = new ConfigBuilder().Add("p.include", "foo").Add("p.include.tags"
				, "foo:f").Add("p.exclude", "bar").Add("p.exclude.tags", "bar:b").Subset("p");
			ShouldAccept(c, "foo");
			ShouldAccept(c, Arrays.AsList(Interns.Tag("foo", string.Empty, "f")));
			ShouldAccept(c, MockMetricsRecord("foo", Arrays.AsList(Interns.Tag("foo", string.Empty
				, "f"))));
			ShouldReject(c, "bar");
			ShouldReject(c, Arrays.AsList(Interns.Tag("bar", string.Empty, "b")));
			ShouldReject(c, MockMetricsRecord("bar", Arrays.AsList(Interns.Tag("foo", string.Empty
				, "f"))));
			ShouldReject(c, MockMetricsRecord("foo", Arrays.AsList(Interns.Tag("bar", string.Empty
				, "b"))));
			ShouldAccept(c, "foobar");
			ShouldAccept(c, Arrays.AsList(Interns.Tag("foobar", string.Empty, string.Empty)));
			ShouldAccept(c, MockMetricsRecord("foobar", Arrays.AsList(Interns.Tag("foobar", string.Empty
				, string.Empty))));
		}

		/// <summary>Include patterns should take precedence over exclude patterns</summary>
		[Fact]
		public virtual void IncludeShouldOverrideExclude()
		{
			SubsetConfiguration c = new ConfigBuilder().Add("p.include", "foo").Add("p.include.tags"
				, "foo:f").Add("p.exclude", "foo").Add("p.exclude.tags", "foo:f").Subset("p");
			ShouldAccept(c, "foo");
			ShouldAccept(c, Arrays.AsList(Interns.Tag("foo", string.Empty, "f")));
			ShouldAccept(c, MockMetricsRecord("foo", Arrays.AsList(Interns.Tag("foo", string.Empty
				, "f"))));
		}

		internal static void ShouldAccept(SubsetConfiguration conf, string s)
		{
			Assert.True("accepts " + s, NewGlobFilter(conf).Accepts(s));
			Assert.True("accepts " + s, NewRegexFilter(conf).Accepts(s));
		}

		// Version for one tag:
		internal static void ShouldAccept(SubsetConfiguration conf, IList<MetricsTag> tags
			)
		{
			ShouldAcceptImpl(true, conf, tags, new bool[] { true });
		}

		// Version for multiple tags: 
		internal static void ShouldAccept(SubsetConfiguration conf, IList<MetricsTag> tags
			, bool[] expectedAcceptedSpec)
		{
			ShouldAcceptImpl(true, conf, tags, expectedAcceptedSpec);
		}

		// Version for one tag:
		internal static void ShouldReject(SubsetConfiguration conf, IList<MetricsTag> tags
			)
		{
			ShouldAcceptImpl(false, conf, tags, new bool[] { false });
		}

		// Version for multiple tags: 
		internal static void ShouldReject(SubsetConfiguration conf, IList<MetricsTag> tags
			, bool[] expectedAcceptedSpec)
		{
			ShouldAcceptImpl(false, conf, tags, expectedAcceptedSpec);
		}

		private static void ShouldAcceptImpl(bool expectAcceptList, SubsetConfiguration conf
			, IList<MetricsTag> tags, bool[] expectedAcceptedSpec)
		{
			MetricsFilter globFilter = NewGlobFilter(conf);
			MetricsFilter regexFilter = NewRegexFilter(conf);
			// Test acceptance of the tag list:  
			Assert.Equal("accepts " + tags, expectAcceptList, globFilter.Accepts
				(tags));
			Assert.Equal("accepts " + tags, expectAcceptList, regexFilter.
				Accepts(tags));
			// Test results on each of the individual tags:
			int acceptedCount = 0;
			for (int i = 0; i < tags.Count; i++)
			{
				MetricsTag tag = tags[i];
				bool actGlob = globFilter.Accepts(tag);
				bool actRegex = regexFilter.Accepts(tag);
				Assert.Equal("accepts " + tag, expectedAcceptedSpec[i], actGlob
					);
				// Both the filters should give the same result:
				Assert.Equal(actGlob, actRegex);
				if (actGlob)
				{
					acceptedCount++;
				}
			}
			if (expectAcceptList)
			{
				// At least one individual tag should be accepted:
				Assert.True("No tag of the following accepted: " + tags, acceptedCount
					 > 0);
			}
			else
			{
				// At least one individual tag should be rejected: 
				Assert.True("No tag of the following rejected: " + tags, acceptedCount
					 < tags.Count);
			}
		}

		/// <summary>Asserts that filters with the given configuration accept the given record.
		/// 	</summary>
		/// <param name="conf">SubsetConfiguration containing filter configuration</param>
		/// <param name="record">MetricsRecord to check</param>
		internal static void ShouldAccept(SubsetConfiguration conf, MetricsRecord record)
		{
			Assert.True("accepts " + record, NewGlobFilter(conf).Accepts(record
				));
			Assert.True("accepts " + record, NewRegexFilter(conf).Accepts(record
				));
		}

		internal static void ShouldReject(SubsetConfiguration conf, string s)
		{
			Assert.True("rejects " + s, !NewGlobFilter(conf).Accepts(s));
			Assert.True("rejects " + s, !NewRegexFilter(conf).Accepts(s));
		}

		/// <summary>Asserts that filters with the given configuration reject the given record.
		/// 	</summary>
		/// <param name="conf">SubsetConfiguration containing filter configuration</param>
		/// <param name="record">MetricsRecord to check</param>
		internal static void ShouldReject(SubsetConfiguration conf, MetricsRecord record)
		{
			Assert.True("rejects " + record, !NewGlobFilter(conf).Accepts(record
				));
			Assert.True("rejects " + record, !NewRegexFilter(conf).Accepts(
				record));
		}

		/// <summary>Create a new glob filter with a config object</summary>
		/// <param name="conf">the config object</param>
		/// <returns>the filter</returns>
		public static GlobFilter NewGlobFilter(SubsetConfiguration conf)
		{
			GlobFilter f = new GlobFilter();
			f.Init(conf);
			return f;
		}

		/// <summary>Create a new regex filter with a config object</summary>
		/// <param name="conf">the config object</param>
		/// <returns>the filter</returns>
		public static RegexFilter NewRegexFilter(SubsetConfiguration conf)
		{
			RegexFilter f = new RegexFilter();
			f.Init(conf);
			return f;
		}

		/// <summary>Creates a mock MetricsRecord with the given name and tags.</summary>
		/// <param name="name">String name</param>
		/// <param name="tags">List<MetricsTag> tags</param>
		/// <returns>MetricsRecord newly created mock</returns>
		private static MetricsRecord MockMetricsRecord(string name, IList<MetricsTag> tags
			)
		{
			MetricsRecord record = Org.Mockito.Mockito.Mock<MetricsRecord>();
			Org.Mockito.Mockito.When(record.Name()).ThenReturn(name);
			Org.Mockito.Mockito.When(record.Tags()).ThenReturn(tags);
			return record;
		}
	}
}
