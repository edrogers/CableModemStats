library(lubridate)
library(dplyr)
library(GGally)
library(ggplot2)

speeds <- read.table("./speeds/speeds.csv",
                     header = TRUE,
                     sep = ",",
                     na.strings = "N/A",
                     colClasses = c("numeric",
                                    "numeric",
                                    "numeric",
                                    "numeric",
                                    "factor",
                                    "factor",
                                    "numeric"))

speeds <- speeds %>% arrange(Time)
speeds$HoursSinceUnixEpoch <- round(speeds$Time/3600,digits = 0)
speeds$Time <- as.POSIXct(speeds$Time,origin = "1970-01-01",tz="America/Chicago")

modem <- read.table("./modemOutput/modemData.csv",
                    header = FALSE,
                    sep=",",
                    na.strings = "n/a",
                    col.names=c("Time",
                                "Downstream Channel ID A",
                                "Downstream Channel ID B",
                                "Downstream Channel ID C",
                                "Downstream Channel ID D",
                                "Downstream Frequency A",
                                "Downstream Frequency B",
                                "Downstream Frequency C",
                                "Downstream Frequency D",
                                "Downstream Signal to Noise A",
                                "Downstream Signal to Noise B",
                                "Downstream Signal to Noise C",
                                "Downstream Signal to Noise D",
                                "Downstream Modulation A",
                                "Downstream Modulation B",
                                "Downstream Modulation C",
                                "Downstream Modulation D",
                                "Downstream Power Level A",
                                "Downstream Power Level B",
                                "Downstream Power Level C",
                                "Downstream Power Level D",
                                "Upstream Channel ID",
                                "Upstream Frequency",
                                "Upstream Ranging Service ID",
                                "Upstream Symbol Rate",
                                "Upstream Power Level",
                                "Upstream Modulation A1",
                                "Upstream Modulation A2",
                                "Upstream Modulation B1",
                                "Upstream Modulation B2",
                                "Upstream Ranging Status",
                                "Signal Stats Channel ID A",
                                "Signal Stats Channel ID B",
                                "Signal Stats Channel ID C",
                                "Signal Stats Channel ID D",
                                "Total Unerrored Codewords A",
                                "Total Unerrored Codewords B",
                                "Total Unerrored Codewords C",
                                "Total Unerrored Codewords D",
                                "Total Correctable Codewords A",
                                "Total Correctable Codewords B",
                                "Total Correctable Codewords C",
                                "Total Correctable Codewords D",
                                "Total Uncorrectable Codewords A",
                                "Total Uncorrectable Codewords B",
                                "Total Uncorrectable Codewords C",
                                "Total Uncorrectable Codewords D"),
                    colClasses = c("numeric",
                                   rep("factor",4),
                                   rep("factor",4),
                                   rep("numeric",4),
                                   rep("factor",4),
                                   rep("numeric",4),
                                   "factor",
                                   "factor",
                                   "factor",
                                   "factor",
                                   "numeric",
                                   rep("factor",4),
                                   "factor",
                                   rep("factor",4),
                                   rep("numeric",4),
                                   rep("numeric",4),
                                   rep("numeric",4))
                    )

modem <- modem %>% arrange(Time)
modem$HoursSinceUnixEpoch <- round(modem$Time/3600,digits = 0)
modem$Time <- as.POSIXct(modem$Time,origin = "1970-01-01",tz="America/Chicago")

modem_transform_downstream <- modem %>% group_by(HoursSinceUnixEpoch) %>% 
  summarize(Downstream.Power = mean(c(Downstream.Power.Level.A,
                                      Downstream.Power.Level.B,
                                      Downstream.Power.Level.C,
                                      Downstream.Power.Level.D),na.rm = TRUE)) %>%
  ungroup()
modem_transform_upstream <- modem %>% group_by(HoursSinceUnixEpoch) %>% 
  summarize(Upstream.Power.Level = mean(Upstream.Power.Level,na.rm=TRUE)) %>%
  ungroup()
modem_transform <- full_join(modem_transform_downstream,
                             modem_transform_upstream,
                             by="HoursSinceUnixEpoch")

speeds_transform <- speeds[speeds$Ping..ms.<4000,
                           c("HoursSinceUnixEpoch","Host.Location","Download.Speed..Mbits.s.","Upload.Speed..Mbits.s.","Ping..ms.")]
speeds_transform <- speeds_transform %>% group_by(HoursSinceUnixEpoch,Host.Location) %>%
  summarise_each(funs(mean(.,na.rm = TRUE))) %>%
  ungroup()

speed.and.power <- inner_join(modem_transform,speeds_transform,by="HoursSinceUnixEpoch")

ggpairs(speed.and.power,color="Host.Location")

speeds_madison <- speeds_transform[speeds_transform$Host.Location=="Madison; WI" & complete.cases(speeds_transform),]
speed.and.power.madison <- inner_join(modem_transform,speeds_madison,by="HoursSinceUnixEpoch")
speed.and.power.madison <- speed.and.power.madison %>% mutate(Ping..ms. = log(Ping..ms.))
speed.and.power.madison$PingTransition <- as.factor(speed.and.power.madison$HoursSinceUnixEpoch > 398158)
ggpairs(speed.and.power.madison,
        columns = c(1,2,3,5,6,7),
        color="PingTransition",
        params=c(alpha=4/10,size=8/10))

powerModel1 <- lm(Download.Speed..Mbits.s. ~ Downstream.Power,speed.and.power.madison)
powerModel2 <- lm(Download.Speed..Mbits.s. ~ Downstream.Power + I(Downstream.Power^2),speed.and.power.madison)
ggplot(data = speed.and.power.madison,
       aes(x=Downstream.Power,y=Download.Speed..Mbits.s.)) + 
  geom_point(alpha=0.3) +
  stat_smooth(method="lm",col="blue")

ggplot(data = speed.and.power.madison,
       aes(x=Upstream.Power.Level,y=Download.Speed..Mbits.s.)) + 
  geom_point(alpha=0.3) +
  stat_smooth(method="lm",col="blue")

ggplot(data = speed.and.power.madison,
       aes(x=Downstream.Power,y=Upstream.Power.Level)) + 
  geom_point(alpha=0.3) +
  stat_smooth(method="lm",col="blue")

powerModel2 <- lm(Download.Speed..Mbits.s. ~ Downstream.Power + 
                                             Upstream.Power.Level + 
                                             Ping..ms.,
                  speed.and.power.madison)
summary(powerModel2)
