package org.gigaspaces.blueprints;

import com.gigaspaces.internal.utils.yaml.YamlUtils;
import com.gigaspaces.internal.version.PlatformVersion;
import com.gigaspaces.start.SystemLocations;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.swagger.codegen.cmd.ConfigHelp;
import io.swagger.codegen.cmd.Generate;
import io.swagger.codegen.cmd.Langs;
import io.swagger.codegen.cmd.Meta;
import io.swagger.codegen.cmd.Validate;
import io.swagger.codegen.cmd.Version;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Niv Ingberg
 * @since 14.5
 */
public class Blueprint {
    private static final String TEMPLATES_PATH = "templates";
    private static final String INFO_PATH = "blueprint.yaml";
    private static final String VALUES_PATH = "values.yaml";

    private final String name;
    private final Path content;
    private final Path valuesPath;
    private final Map<String, String> properties;
    private Map<String, String> values;

    public Blueprint(Path home) {
        this.name = home.getFileName().toString();
        this.content = home.resolve(TEMPLATES_PATH);
        this.valuesPath = home.resolve(VALUES_PATH);
        try {
            this.properties = YamlUtils.toMap(YamlUtils.parse(home.resolve(INFO_PATH)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load blueprint information" , e);
        }
    }

    public static boolean isValid(Path path) {
        return Files.exists(path) &&
                Files.exists(path.resolve(TEMPLATES_PATH)) &&
                Files.exists(path.resolve(INFO_PATH)) &&
                Files.exists(path.resolve(VALUES_PATH));
    }

    public static Collection<Blueprint> fromPath(Path path) throws IOException {
        return Files.list(path)
                .filter(Blueprint::isValid)
                .sorted()
                .map(Blueprint::new)
                .collect(Collectors.toList());
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return getInfo("description");
    }

    public String getInfo(String key) {
        return properties.get(key);
    }

    public Map<String, String> getValues() throws IOException {
        if (values == null) {
            values = YamlUtils.toMap(YamlUtils.parse(valuesPath));
        }
        return values;
    }

    public Path getDefaultTarget(){
        String name = "my-" + getName();
        int suffix = 1;
        Path path;
        for (path = Paths.get(name) ; Files.exists(path); path = Paths.get(name + suffix++));
        return path;
    }

    public void generate(Path target) throws IOException {
        generate(target, Collections.emptyMap());
    }

    public void generate(Path target, Map<String, String> valuesOverrides) throws IOException {
        if (Files.exists(target))
            throw new IllegalArgumentException("Target already exists: " + target);

        Map<String, String> properties = merge(valuesOverrides);

        TemplateUtils.evaluateTree(content, target, tryParse(properties));
        generateModelsFromSwagger(target, properties);
    }

    private void generateModelsFromSwagger(Path target, Map<String, String> properties) {
        Cli.CliBuilder<Runnable> builder =
            Cli.<Runnable>builder("swagger-codegen-cli")
                .withDefaultCommand(Langs.class)
                .withCommands(Generate.class, Meta.class, Langs.class, Help.class,
                    ConfigHelp.class, Validate.class, Version.class);

        if (properties.get("swagger") != null && !properties.get("swagger").isEmpty()) {
            String modelPackage = properties.get("project.groupId") + ".model";
            String separator = System.getProperty("file.separator");
            String[] args = new String[]{
                "generate",
                "-i", properties.get("swagger"),
                "--model-package", modelPackage,
                "-l", "java",
                "--library", "feign",
                "-Dmodels",
                "-DmodelTests=false",
                "-DmodelDocs=false",
                "-DhideGenerationTimestamp=true",
                "-DdateLibrary=java8",
                "-o", target.toFile().getAbsolutePath()};
            System.out.println(target.toFile().getAbsolutePath());
            builder.build().parse(args).run();
            String path =
                target.toFile().getAbsolutePath() + separator + "src" + separator + "main" + separator + "java"
                    + separator + modelPackage.replaceAll("\\.", separator);
            deleteAnnotations(path);
        }
    }

    private void deleteAnnotations(String path) {
        System.out.println(path);

        File[] files = new File(path).listFiles();

        Charset charset = StandardCharsets.UTF_8;
        for (File file : files) {
            String content;
            try {
                content = new String(Files.readAllBytes(file.toPath()), charset);
                content = content.replaceAll(".*JsonProperty.*\n", "");
                content = content.replaceAll(".*JsonCreator.*\n", "");
                content = content.replaceAll(".*JsonValue.*\n", "");
                content = content.replaceAll(".*ApiModel.*\n", "");
                content = content.replaceAll(".*ApiModelProperty.*\n", "");
                Files.write(file.toPath(), content.getBytes(charset));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Map<String, String> merge(Map<String, String> overrides) throws IOException {
        Map<String, String> merged = new LinkedHashMap<>(getValues());
        if (overrides != null)
            merged.putAll(overrides);
        merged.putIfAbsent("gs.version", PlatformVersion.getInstance().getId());
        merged.putIfAbsent("gs.home", SystemLocations.singleton().home().toString());
        if (!merged.containsKey("project.package-path")) {
            String groupId = merged.get("project.groupId");
            if (groupId != null)
                merged.put("project.package-path", groupId.replace(".", File.separator));
        }
        return merged;
    }

    private static Map<String, Object> tryParse(Map<String, String> map) {
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach((k, v) -> result.put(k, tryParse(v)));
        return result;
    }

    private static Object tryParse(String s) {
        if (Boolean.FALSE.toString().equals(s))
            return Boolean.FALSE;
        if (Boolean.TRUE.toString().equals(s))
            return Boolean.TRUE;
        return s;
    }

}
