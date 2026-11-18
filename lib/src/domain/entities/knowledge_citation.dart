/// Immutable value object representing a Knowledge Base document citation.
final class KnowledgeCitation {
  final String id;
  final String title;
  final String? snippet;
  final String? urlOrPath;
  final int? pageNumber;
  final double? relevanceScore;
  final Map<String, dynamic> metadata;

  const KnowledgeCitation({
    required this.id,
    required this.title,
    this.snippet,
    this.urlOrPath,
    this.pageNumber,
    this.relevanceScore,
    this.metadata = const {},
  });

  factory KnowledgeCitation.fromJson(Map<String, dynamic> json) {
    return KnowledgeCitation(
      id: (json['id'] ?? json['doc_id'] ?? json['source_id'] ?? '').toString(),
      title: (json['title'] ??
              json['document_title'] ??
              json['name'] ??
              'Knowledge Source')
          .toString(),
      snippet: (json['snippet'] ??
              json['cited_text'] ??
              json['quote'] ??
              json['content'])
          ?.toString(),
      urlOrPath: (json['url'] ?? json['path'] ?? json['uri'])?.toString(),
      pageNumber: json['page'] is int
          ? json['page'] as int
          : int.tryParse('${json['page'] ?? json['page_number'] ?? ''}'),
      relevanceScore: (json['score'] ?? json['relevance_score']) is num
          ? ((json['score'] ?? json['relevance_score']) as num).toDouble()
          : null,
      metadata: json,
    );
  }

  Map<String, dynamic> toJson() => {
        'id': id,
        'title': title,
        if (snippet != null) 'snippet': snippet,
        if (urlOrPath != null) 'urlOrPath': urlOrPath,
        if (pageNumber != null) 'pageNumber': pageNumber,
        if (relevanceScore != null) 'relevanceScore': relevanceScore,
      };
}
