FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .
WORKDIR /src/examples/signalsbot
RUN dotnet publish -c Release -o /out

FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /out .
RUN mkdir -p /data
ENV SIGNALS_DB_PATH=/data/signalsbot.json
ENTRYPOINT ["dotnet", "SignalsBot.Example.dll"]
CMD ["papertrader"]
