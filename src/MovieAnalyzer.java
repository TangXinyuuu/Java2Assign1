import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieAnalyzer {
    Stream<Movie> movieStream;
    List<Movie> movies;

    public static class Movie {
        String series_title;
        Integer released_year;
        String certificate;
        float runtime;
        List<String> genre;
        float IMDB_Rating;
        String overview;
        float meta_score;
        String director;

        List<String> stars;
        long noofvotes;
        long gross;

        public Movie(String series_title, Integer released_year, String certificate, float runtime,
                     List<String> genre, float IMDB_Rating, String overview, float meta_score,
                     String director, List<String> stars, long noofvotes, long gross) {
            this.series_title = series_title;
            this.released_year = released_year;
            this.certificate = certificate;
            this.runtime = runtime;
            this.genre = genre;
            this.IMDB_Rating = IMDB_Rating;
            this.overview = overview;
            this.meta_score = meta_score;
            this.director = director;
            this.stars = stars;
            this.noofvotes = noofvotes;
            this.gross = gross;
        }

        public Integer getReleased_year() {
            return released_year;
        }

        public List<String> getGenre() {
            return genre;
        }

        public List<String> getStars() {
            return stars;
        }
    }

    public MovieAnalyzer(String dataset_path) throws IOException {

        movieStream = Files.lines(Paths.get(dataset_path))
                .skip(1)
                .map(MovieAnalyzer::parseCSVLine)
                .map(MovieAnalyzer::newMovie);
        movies = movieStream.toList();

    }


    public Map<Integer, Integer> getMovieCountByYear() {
        Map<Integer, Integer> movieCountByYear_pre = movies.stream().collect(Collectors.groupingBy(Movie::getReleased_year, Collectors.summingInt(e -> 1)));
        Map<Integer, Integer> movieCountByYear = new TreeMap<Integer, Integer>(Collections.reverseOrder());
        movieCountByYear.putAll(movieCountByYear_pre);
        return movieCountByYear;
    }

    public Map<String, Integer> getMovieCountByGenre() {
        Map<String, Integer> movieCountByGenre_pre = new HashMap<>();
        movies.stream().forEach(a -> {
            for (String s : a.getGenre()) {
                movieCountByGenre_pre.merge(s, 1, Integer::sum);
            }
        });

        SortedSet<Map.Entry<String, Integer>> sortedSet = new TreeSet<>((e1, e2) -> {
            int res = -(e1.getValue().compareTo(e2.getValue()));
            if (res == 0)
                return e1.getKey().compareTo(e2.getKey());
            return res;
        });
        sortedSet.addAll(movieCountByGenre_pre.entrySet());
        Map<String, Integer> movieCountByGenre = new LinkedHashMap<>();
        for (var entry : sortedSet) movieCountByGenre.put(entry.getKey(), entry.getValue());
        return movieCountByGenre;
    }

    public Map<List<String>, Integer> getCoStarCount() {
        Map<List<String>, Integer> coStarCount = new HashMap<>();

        movies.stream().forEach(a -> {
            for (int i = 0; i < a.getStars().size(); i++) {
                String s1 = a.getStars().get(i);
                for (int j = 0; j < a.getStars().size(); j++) {
                    String s2 = a.getStars().get(j);
                    if (i != j) {
                        List<String> tmp = new ArrayList<>();
                        if (s1.compareTo(s2) > 0) {
                            tmp.add(s2);
                            tmp.add(s1);
                        } else {
                            tmp.add(s1);
                            tmp.add(s2);
                        }
                        coStarCount.merge(tmp, 1, Integer::sum);
//                        if (coStarCount.get(tmp) != null){
//                            coStarCount.put(tmp, coStarCount.get(tmp) + 1);
//                        }else {
//                            coStarCount.put(tmp, 1);
//                        }

                    }
                }
            }
        });
//        Map<List<String>, Integer> coStarCount = new HashMap<>();
//        List<Map.Entry<List<String>, Integer>> list = coStarCount_pre.entrySet().stream().sorted((a, b)-> -(a.getValue().compareTo(b.getValue()))).toList();
        for (Map.Entry<List<String>, Integer> entry: coStarCount.entrySet()){
            coStarCount.put(entry.getKey(), entry.getValue()/2);
        }

        return coStarCount;
    }

    public List<String> getTopMovies(int top_k, String by) {
        List<String> topMovies = new ArrayList<>();
        if (by.equals("runtime")) {
            List<Movie> list = movies.stream().sorted((a, b) -> {
                if (a.runtime > b.runtime) {
                    return -1;
                } else if (a.runtime < b.runtime) {
                    return 1;
                } else {
                    return a.series_title.compareTo(b.series_title);
                }
            }).limit(top_k).toList();
            for (Movie m : list) {
                topMovies.add(m.series_title);
            }
        } else if (by.equals("overview")) {
            List<Movie> list = movies.stream().sorted((a, b) -> {
                if (a.overview.length() > b.overview.length()) {
                    return -1;
                } else if (a.overview.length() < b.overview.length()) {
                    return 1;
                } else {
                    return a.series_title.compareTo(b.series_title);
                }
            }).limit(top_k).toList();
            for (Movie m : list) {
                topMovies.add(m.series_title);
            }
        }
        return topMovies;
    }

    public List<String> getTopStars(int top_k, String by) {
        List<String> topStars = new ArrayList<>();
        Map<String, Double> stars_res = new HashMap<>();
        Map<String, Integer> stars_mNum = new HashMap<>();
        if(by.equals("rating")) {
            movies.stream().forEach(a -> {
                if (a.IMDB_Rating != -1) {
                    for (String star : a.getStars()) {
                        if (stars_mNum.get(star) == null) {
                            stars_mNum.put(star, 1);
                            stars_res.put(star, (double) a.IMDB_Rating);
                        } else {
                            stars_mNum.put(star, stars_mNum.get(star) + 1);
                            stars_res.put(star, stars_res.get(star) + (double) a.IMDB_Rating);
                        }
                    }
                }
            });
        } else if (by.equals("gross")){
            movies.stream().forEach(a -> {
                if (a.gross != -1) {
                    for (String star : a.getStars()) {
                        if (stars_mNum.get(star) == null) {
                            stars_mNum.put(star, 1);
                            stars_res.put(star, (double) a.gross);
                        } else {
                            stars_mNum.put(star, stars_mNum.get(star) + 1);
                            stars_res.put(star, stars_res.get(star) + (double) a.gross);
                        }
                    }
                }
            });
        }
        for (String key : stars_res.keySet()){
            stars_res.replace(key, stars_res.get(key)/stars_mNum.get(key));
        }
        List<Map.Entry<String, Double>> list = stars_res.entrySet().stream().sorted((a, b) -> {
            if(a.getValue().compareTo(b.getValue())!=0){
                return a.getValue().compareTo(b.getValue()) * (-1);
            }else {
                return a.getKey().compareTo(b.getKey());
            }
        }).limit(top_k).toList();
        for (Map.Entry<String, Double> entry: list){
            topStars.add(entry.getKey());
        }
        return topStars;
    }

    public List<String> searchMovies(String genre, float min_rating, int max_runtime) {
        List<String> movies_name = new ArrayList<>();
        movies.stream().forEach(a -> {
            int judge = 0;
            for (String s:a.genre){
                if(s.equals(genre)){
                    judge = 1;
                }
            }
            if(judge == 1 && a.IMDB_Rating >= min_rating && a.runtime <= max_runtime){
                movies_name.add(a.series_title);
            }
        });
        Collections.sort(movies_name);
        return movies_name;
    }

    public static String[] parseCSVLine(String line) {
        Pattern p = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");
        String[] fields = p.split(line);
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].startsWith("\"")) {
                fields[i] = fields[i].substring(1);
            }
            if (fields[i].endsWith("\"")) {
                fields[i] = fields[i].substring(0, fields[i].length() - 1);
            }
        }
        return fields;
    }

    public static Movie newMovie(String[] a) {
        Movie movie;
        if (a.length == 16) {
            movie = new Movie(a[1], Integer.parseInt(a[2]), a[3], getRuntime(a[4]),
                    getGenre(a[5]), getScore(a[6]), getOverview(a[7]), getScore(a[8]), a[9],
                    getStars(a[10], a[11], a[12], a[13]), getNoOfVotes(a[14]), getGross(a[15]));
        } else {
            movie = new Movie(a[1], Integer.parseInt(a[2]), a[3], getRuntime(a[4]),
                    getGenre(a[5]), getScore(a[6]), getOverview(a[7]), getScore(a[8]), a[9],
                    getStars(a[10], a[11], a[12], a[13]), getNoOfVotes(a[14]), -1);
        }
        return movie;

    }

    public static int getGross(String s) {
        if (s == null || s.equals("")) {
            return -1;
        } else {
            String[] parts = s.replace("\"", "").split(",");
            StringBuilder res = new StringBuilder();
            for (String part : parts) {
                res.append(part);
            }
            return Integer.parseInt(res.toString());
        }
    }

    public static int getNoOfVotes(String s) {
        if (s == null || s.equals("")) {
            return -1;
        } else {
            return Integer.parseInt(s);
        }
    }

    public static List<String> getStars(String s1, String s2, String s3, String s4) {
        List<String> stars = new ArrayList<>();
        if (s1 != null && !s1.equals("")) {
            stars.add(s1);
        }

        if (s2 != null && !s2.equals("")) {
            stars.add(s2);
        }

        if (s3 != null && !s3.equals("")) {
            stars.add(s3);
        }

        if (s4 != null && !s4.equals("")) {
            stars.add(s4);
        }
        return stars;
    }

    public static float getScore(String s) {
        if (s == null || s.equals("")) {
            return -1;
        } else {
            return Float.parseFloat(s);
        }
    }


    public static float getRuntime(String s) {
        if (s == null || s.equals("")) {
            return -1;
        } else {
            return Float.parseFloat(s.split(" ")[0]);
        }

    }

    public static List<String> getGenre(String str) {
        return Arrays.stream(str.replace("\"", "").split(", ")).toList();
    }

    public static String getOverview(String str) {
        if (str.startsWith("\"")) {
            str = str.substring(1);
        }
        if (str.endsWith("\"")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }


}

