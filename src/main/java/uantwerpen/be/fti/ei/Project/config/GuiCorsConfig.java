package uantwerpen.be.fti.ei.Project.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@Profile("namingserver")
public class GuiCorsConfig implements WebMvcConfigurer {
    @Override public void addCorsMappings(CorsRegistry r){
        r.addMapping("/api/**").allowedMethods("*").allowedOrigins("*");
        r.addMapping("/gui/**").allowedMethods("*").allowedOrigins("*");
    }
}