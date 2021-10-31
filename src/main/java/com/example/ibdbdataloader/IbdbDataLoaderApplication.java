package com.example.ibdbdataloader;

import com.example.ibdbdataloader.author.Author;
import com.example.ibdbdataloader.author.AuthorRepository;
import com.example.ibdbdataloader.book.Book;
import com.example.ibdbdataloader.book.BookRepository;
import com.example.ibdbdataloader.connection.DataStaxAstraProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class IbdbDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;
	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(IbdbDataLoaderApplication.class, args);
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties dataStaxAstraProperties) {
		Path bundle = dataStaxAstraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

	private void initAuthors() {
		Path path = Paths.get(authorDumpLocation);
		try(Stream<String> lines = Files.lines(path)) {
			lines.forEach( line -> {
				//read and parse line
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					//construct author object
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", ""));
					//persist object
					System.out.println("saving author "+ author.getName() + "...");
					authorRepository.save(author);

				} catch (JSONException jsonException) {
					jsonException.printStackTrace();
				}
			});
		}catch (IOException e) {
			e.getStackTrace();
		}
	}

	private void initWorks() {
		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try(Stream<String> lines = Files.lines(path)) {
			lines.forEach( line -> {
				//read and parse line
				String jsonString = line.substring(line.indexOf("{"));
				System.out.println(jsonString);
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					//construct author object
					Book book = new Book();
					book.setId(jsonObject.optString("key").replace("/works/", ""));
					book.setName(jsonObject.optString("title"));
					JSONObject descObject = jsonObject.optJSONObject("description");
					if (descObject != null) {
						book.setDescription(descObject.optString("value"));
					}
					JSONObject publishedObject = jsonObject.optJSONObject("created");
					if (publishedObject != null) {
						book.setPublishedDate(LocalDate.parse(publishedObject.optString("value"), dateTimeFormatter));
					}
					JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
					if (coversJSONArr != null) {
						List<String> coversIds = new ArrayList<>();
						for (int i = 0; i <coversJSONArr.length() ; i++) {
							coversIds.add(coversJSONArr.getString(i));
						}
						book.setCoverIds(coversIds);
					}
					JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
					if (authorsJSONArr != null) {
						List<String> authorIds = new ArrayList<>();
						for (int i = 0; i <authorsJSONArr.length() ; i++) {
								String authorId = authorsJSONArr.getJSONObject(i).getJSONObject("author").getString("key")
										.replace("/authors/", "");
								authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);
						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent()) return "Unknown Author";
									return optionalAuthor.get().getName();
								}).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
					}


					//persist object
					System.out.println("saving book "+ book.getName() + "...");
					bookRepository.save(book);

				} catch (JSONException jsonException) {
					jsonException.printStackTrace();
				}
			});
		}catch (IOException e) {
			e.getStackTrace();
		}
	}

	@PostConstruct
	public void Start() {
		System.out.println("start method");
		/*Author author = new Author();
		author.setId("id");
		author.setName("id");
		author.setPersonalName("id");
		authorRepository.save(author);*/

		//initAuthors();
		initWorks();
	}
}
