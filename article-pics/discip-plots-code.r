for(i in 1:760){
    reg_data[i, "discip"] <- discips[reg_data[i, "discip"], "display_name"]
}

ggplot() +
    geom_smooth(data = reg_data,
                mapping = aes(x = time, y = repo_ratio, color = "P"),
                show.legend = TRUE) +
    geom_smooth(data = reg_data,
                mapping = aes(x = time, y = I(ex_log - log_med), color = "Y"),
                show.legend = TRUE) +
    scale_color_manual(values = c("P" = "blue", "Y" = "red")) +
    facet_wrap(vars(discip), scales = "free_y", ncol = 4) +
    theme(
        axis.text = element_text(size = 7),
        axis.title = element_blank(),
        legend.justification = c(1,1),
        legend.position = c(0.95,0.14),
        legend.title = element_blank(),
        strip.text.x = element_text(size = 8)
    )